// SPDX-FileCopyrightText: 2023 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

const Util = require("../common-util");
const Store = require("../storage/file");
const BlobStore = require("../storage/blob");
const Tasks = require("../storage/tasks");
const nThen = require("nthen");
const Logger = require("../log");
const HK = require('../hk-util');

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

const readHistoryMessages = (data, cb) => {
    const { channel, offset, beforeHash } = data;

    const start = (beforeHash) ? 0 : offset;
    store.readMessagesBin(channel, start, (msgObj, readMore, abort) => {
        if (beforeHash && msgObj.offset >= offset) { return void abort(); }
        const parsed = HK.tryParse(Env, msgObj.buff.toString('utf8'));
        if (!parsed) console.log(channel);
        if (!parsed) { return void readMore(); }

        // 3rd argument: this is a handler with a readMore cb
        cb(void 0, parsed, () => {
            readMore();
        });
    }, (err, reason) => {
        cb(err, reason);
    });
};





const COMMANDS = {
    STORE_MESSAGE: storeMessage,
    READ_HISTORY_MESSAGES: readHistoryMessages
};



// Store readMore callback for a given txid
const readMore = {};


process.on('message', function (data) {
    if (!data || !data.txid || !data.pid) {
        return void process.send({
            error:'E_INVAL',
            data: data,
        });
    }

    // If this is a readMore command, call its associated readMore function
    if (data.readMore) {
        if (typeof(readMore[data.txid]) === "function") { readMore[data.txid](); }
        return;
    }

    // Otherwise treat it as a first command

    const cb = function (err, value, isHandler) {
        if (isHandler) {
            readMore[data.txid] = isHandler;
        } else {
            delete readMore[data.txid];
        }
        process.send({
            error: Util.serializeError(err),
            txid: data.txid,
            pid: data.pid,
            value: value,
            handler: Boolean(data.hasHandler && isHandler)
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
