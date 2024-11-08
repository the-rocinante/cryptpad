// SPDX-FileCopyrightText: 2023 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

var Fs = require('fs');
var nThen = require("nthen");
var Util = require("./lib/common-util");

var OS = require("node:os");
var Cluster = require("node:cluster");

var config = require("./lib/load-config");
var Environment = require("./lib/env");
var Env = Environment.create(config);
var Default = require("./lib/defaults");

(function () {
    // you absolutely must provide an 'httpUnsafeOrigin' (a truthy string)
    if (typeof(Env.httpUnsafeOrigin) !== 'string' || !Env.httpUnsafeOrigin.trim()) {
        throw new Error("No 'httpUnsafeOrigin' provided");
    }
}());

Object.keys(Env.plugins || {}).forEach(name => {
    let plugin = Env.plugins[name];
    if (!plugin.initialize) { return; }
    try { plugin.initialize(Env, "main"); }
    catch (e) {}
});

var COMMANDS = {};

COMMANDS.LOG = function (msg, cb) {
    var level = msg.level;
    Env.Log[level](msg.tag, msg.info);
    cb();
};

COMMANDS.UPDATE_QUOTA = function (msg, cb) {
    var Quota = require("./lib/commands/quota");
    Quota.updateCachedLimits(Env, (err) => {
        if (err) {
            Env.Log.warn('UPDATE_QUOTA_ERR', err);
            return void cb(err);
        }
        Env.Log.info('QUOTA_UPDATED', {});
        cb();
    });
};

COMMANDS.GET_PROFILING_DATA = function (msg, cb) {
    cb(void 0, Env.bytesWritten);
};

COMMANDS.WS = function (msg, cb) {
    let type = msg.type;
    let data = msg.data;
    let worker = msg.worker;
    let sendMsg;
    if (type === 'open') {
        sendMsg = (data, cb) => {
            Env.sendHttpCommand(worker, 'WS', {
                id: msg.data.id,
                data: data
            }, cb);
        };
    }
    Env.Server.give(type, data, sendMsg);
    setTimeout(cb);
};

Object.keys(Env.plugins || {}).forEach(name => {
    let plugin = Env.plugins[name];
    if (!plugin.addMainCommands) { return; }
    try {
        let commands = plugin.addMainCommands(Env);
        Object.keys(commands || {}).forEach(cmd => {
            // Uppercase command name?
            if (cmd !== cmd.toUpperCase()) { return; }
            // Command is a function?
            if (typeof(commands[cmd]) !== "function") { return; }
            // Command doesn't already exists?
            if (COMMANDS[cmd]) { return; }

            COMMANDS[cmd] = commands[cmd];
        });
    } catch (e) {}
});


nThen(function (w) {
    require("./lib/log").create(config, w(function (_log) {
        Env.Log = _log;
        config.log = _log;
    }));
}).nThen(function (w) {
    Fs.exists("customize", w(function (e) {
        if (e) { return; }
        Env.Log.info('NO_CUSTOMIZE_FOLDER', {
            message: "CryptPad is customizable, see customize.dist/readme.md for details",
        });
    }));
}).nThen(function (w) {
    // check that a valid origin was provided in the config
    try {
        var url = new URL('', Env.httpUnsafeOrigin).href;
        Env.Log.info("WEBSERVER_LISTENING", {
            origin: url,
        });
    } catch (err) {
        Env.Log.error("INVALID_ORIGIN", {
            httpUnsafeOrigin: Env.httpUnsafeOrigin,
        });
        process.exit(1);
    }

    var limit = Env.maxWorkers;
    var workerState = {
        Env: Environment.serialize(Env),
    };

    const response = Util.response(function (errLabel, info) {
        if (!Env.Log) { return; }
        Env.Log.error(errLabel, info);
    });

    Cluster.setupPrimary({
        exec: './lib/http-worker.js',
        args: [],
    });

    var launchWorker = (online) => {
        var worker = Cluster.fork(workerState);
        var pid = worker.process.pid;
        worker.on('online', () => {
            online();
        });

        worker.on('message', msg => {
            if (!msg) { return; }
            var txid = msg.txid;

            if (msg.type === 'REPLY') {
                if (!response.expected(txid)) { return; }
                return void response.handle(txid, [msg.error, msg.content]);
            }

            var content = msg.content;
            if (!content) { return; }
            var command = COMMANDS[content.command];
            if (typeof(command) !== 'function') {
                return void Env.Log.error('UNHANDLED_HTTP_WORKER_COMMAND', msg);
            }

            const cb = Util.once(Util.mkAsync(function (err, value) {
                worker.send({
                    type: 'REPLY',
                    error: Util.serializeError(err),
                    txid: txid,
                    pid: msg.pid,
                    value: value,
                });
            }));

            content.worker = worker;
            command(content, cb);
        });

        worker.on('exit', (code, signal) => {
            Object.keys(Env.plugins || {}).forEach(name => {
                let plugin = Env.plugins[name];
                if (!plugin.onWorkerClosed) { return; }
                try { plugin.onWorkerClosed("http-worker", pid); }
                catch (e) {}
            });

            if (!signal && code === 0) { return; }
            // relaunch http workers if they crash
            Env.Log.error('HTTP_WORKER_EXIT', {
                signal,
                code,
            });
            // update the environment with the latest state before relaunching
            workerState.Env = Environment.serialize(Env);
            launchWorker(function () {
                Env.Log.info('HTTP_WORKER_RELAUNCH', {});
            });
        });
    };

    var txids = {};

    var sendCommand = Env.sendHttpCommand = (worker, command, data, cb) => {
        let txid = Util.guid(txids);
        cb = cb || function () {};
        response.expect(txid, cb);
        worker.send({
            type: 'EVENT',
            txid: txid,
            command: command,
            data: data,
        });
    };

    var broadcast = Env.broadcast = (command, data/*, cb*/) => {
        for (const worker of Object.values(Cluster.workers)) {
            sendCommand(worker, command, data /*, cb */);
        }
        return Object.values(Cluster.workers);
    };

    var throttledEnvChange = Util.throttle(function () {
        Env.Log.info('WORKER_ENV_UPDATE', 'Updating HTTP workers with latest state');
        broadcast('ENV_UPDATE', Environment.serialize(Env));
    }, 250); // NOTE: changing this value will impact lib/commands/admin-rpc.js#adminDecree callback

    var throttledCacheFlush = Util.throttle(function () {
        Env.Log.info('WORKER_CACHE_FLUSH', 'Instructing HTTP workers to flush cache');
        broadcast('FLUSH_CACHE', Env.FRESH_KEY);
    }, 250);

    Env.envUpdated.reg(throttledEnvChange);
    Env.cacheFlushed.reg(throttledCacheFlush);

    OS.cpus().forEach((cpu, index) => {
        if (limit && index >= limit) {
            return;
        }
        launchWorker(w());
    });
}).nThen(function () {
    if (Env.shouldUpdateNode) {
        Env.Log.warn("NODEJS_OLD_VERSION", {
            message: `The CryptPad development team recommends using at least NodeJS v${Default.recommendedVersion.join('.')}`,
            currentVersion: process.version,
        });
    }
    if (Env.OFFLINE_MODE) { return; }
    if (config.externalWebsocketURL) { return; }

    require("./lib/api").create(Env);
});

