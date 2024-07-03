// SPDX-FileCopyrightText: 2023 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

const Util = require("../common-util");
const Store = require("../storage/file");
const BlobStore = require("../storage/blob");
const Tasks = require("../storage/tasks");
const nThen = require("nthen");
const Logger = require("../log");

const Env = {
    Log: {},
};

// support the usual log API but pass it to the main process
Logger.levels.forEach(function (level) {
    Env.Log[level] = function (label, info) {
        process.send({
            log: level,
            label: label,
            info: info,
        });
    };
});

var ready = false;
var store;
var pinStore;
var blobStore;

const init = function (config, _cb) {
    const cb = Util.once(Util.mkAsync(_cb));
    if (!config) {
        return void cb('E_INVALID_CONFIG');
    }

    Env.paths = {
        pin: config.pinPath,
        block: config.blockPath,
    };

    Env.inactiveTime = config.inactiveTime;
    Env.archiveRetentionTime = config.archiveRetentionTime;
    Env.accountRetentionTime = config.accountRetentionTime;

    nThen(function (w) {
        Store.create(config, w(function (err, _store) {
            if (err) {
                w.abort();
                return void cb(err);
            }
            Env.store = store = _store;
        }));
        Store.create({
            filePath: config.pinPath,
            archivePath: config.archivePath,
            // important to initialize the pinstore with its own volume id
            // otherwise archived pin logs will get mixed in with channels
            volumeId: 'pins',
        }, w(function (err, _pinStore) {
            if (err) {
                w.abort();
                return void cb(err);
            }
            Env.pinStore = pinStore = _pinStore;
        }));
        BlobStore.create({
            blobPath: config.blobPath,
            blobStagingPath: config.blobStagingPath,
            archivePath: config.archivePath,
            getSession: function () {},
        }, w(function (err, blob) {
            if (err) {
                w.abort();
                return void cb(err);
            }
            Env.blobStore = blobStore = blob;
        }));
    }).nThen(function (w) {
        Tasks.create({
            log: Env.Log,
            taskPath: config.taskPath,
            store: store,
        }, w(function (err, tasks) {
            if (err) {
                w.abort();
                return void cb(err);
            }
            Env.tasks = tasks;
        }));
    }).nThen(function () {
        cb();
    });
};


const storeMessage = (data, cb) => {
    const id = data.channel;
    const msg = data.msg;
    const Log = Env.Log;

    const msgBin = Buffer.from(msg + '\n', 'utf8');
    store.messageBin(id, msgBin, function (err) {
        if (err) { return void cb(err); }
        cb(void 0, msgBin);
    });
};







const COMMANDS = {
    STORE_MESSAGE: storeMessage
};



process.on('message', function (data) {
    if (!data || !data.txid || !data.pid) {
        return void process.send({
            error:'E_INVAL',
            data: data,
        });
    }

    const cb = function (err, value) {
        process.send({
            error: Util.serializeError(err),
            txid: data.txid,
            pid: data.pid,
            value: value,
        });
    };

    if (!ready) {
        return void init(data.config, function (err) {
            if (err) { return void cb(Util.serializeError(err)); }
            ready = true;
            cb();
        });
    }

    const command = COMMANDS[data.command];
    if (typeof(command) !== 'function') {
        return void cb("E_BAD_COMMAND");
    }
    command(data, cb);
});

process.on('uncaughtException', function (err) {
    console.error('[%s] UNCAUGHT EXCEPTION IN DB WORKER', new Date());
    console.error(err);
    console.error("TERMINATING");
    process.exit(1);
});
