/* jshint esversion: 6 */
const Crypto = require('crypto');

const LAG_MAX_BEFORE_DISCONNECT = 60000;
const LAG_MAX_BEFORE_PING = 15000;

const now = function () { return (new Date()).getTime(); };

const socketSendable = function (socket) {
    return socket && socket.readyState === 1;
};

// Try to keep 4MB of data in queue, if there's more on the buffer, hold off.
const QUEUE_CHR = 1024 * 1024 * 4;

const noop = function () {};

const ADMIN_CHANNEL_LENGTH = 33;

// FIXME there are many circumstances under which call back
// possible cause of a memory leak?
const sendMsg = function (ctx, user, msg, cb) {
    const _cb = function (err) {
        if (typeof(cb) !== 'function') { return; }
        cb(err);
    };

    const stats = ctx.stats;
    // don't bother trying to send if the user doesn't exist anymore
    if (!user) { return void _cb("NO_USER"); }
    // or if you determine that it's unsendable
    if (!socketSendable(user.socket)) { return void _cb("UNSENDABLE"); }
    try {
        const strMsg = JSON.stringify(msg);
        user.inQueue += strMsg.length;
        if (cb) { user.sendMsgCallbacks.push(cb); }
        if (stats) {
            stats.sent = (stats.sent || 0) + 1;
            stats.sentSize = (stats.sentSize || 0) + strMsg.length;
        }
        user.socket.send(strMsg, (err) => {
            user.inQueue -= strMsg.length;
            if (user.inQueue > QUEUE_CHR) { return; }
            const smcb = user.sendMsgCallbacks;
            user.sendMsgCallbacks = [];
            try {
                smcb.forEach((cb)=>{cb();});
            } catch (e) {
                ctx.emit.error(e, 'SEND_MESSAGE_FAIL');
            }
        });
    } catch (e) {
        console.error(e);
        // call back any pending callbacks before you drop the user
        ctx.emit.error(e, 'SEND_MESSAGE_FAIL_2');
        ctx.dropUser(user, 'SEND_MESSAGE_FAIL_2');
    }
};


const WEBSOCKET_CLOSING = 2;
const WEBSOCKET_CLOSED = 3;

const dropUser = function (ctx, user, reason) {
    return;
    if (!user || !user.socket) { return; }
    if (user.socket.readyState !== WEBSOCKET_CLOSING
        && user.socket.readyState !== WEBSOCKET_CLOSED)
    {
        try {
            user.socket.close();
        } catch (e) {
            ctx.emit.error(e, 'FAIL_TO_DISCONNECT', { id: user.id, });
            try {
                user.socket.terminate();
            } catch (ee) {
                ctx.emit.error(ee, 'FAIL_TO_TERMINATE', { id: user.id, });
            }
        }
    }
    delete ctx.users[user.id];
    ctx.emit.sessionClose({
        id: user.id, reason, ip: user.ip
    });
};

const randName = function () { return Crypto.randomBytes(16).toString('hex'); };

const isDefined = function (x) {
    return typeof(x) !== 'undefined';
};


const handlePing = function (ctx, args) {
    sendMsg(ctx, args.user, [args.seq, 'ACK']);
};

const sendMain = type => {
    return (ctx, args) => {
        delete args.user;
        ctx.emit.message({
            type,
            args
        });
    };
};
const commands = {
    JOIN: sendMain('JOIN'),
    MSG: sendMain('MSG'),
    LEAVE: sendMain('LEAVE'),
    PING: handlePing,
};

const handleMessage = function (ctx, user, msg) {
    // this parse is safe because handleMessage
    // is only ever called in a try-catch
    let json = JSON.parse(msg);
    let seq = json.shift();
    let cmd = json[0];

    const stats = ctx.stats;
    if (stats) {
        stats.received = (stats.received || 0) + 1;
        stats.receivedSize = (stats.receivedSize || 0) + msg.length;
    }

    user.timeOfLastMessage = now();
    user.pingOutstanding = false;

    if (typeof(commands[cmd]) !== 'function') { return; }
    commands[cmd](ctx, {
        user: user,
        id: user.id,
        json: json,
        seq: seq,
        obj: json[1],
    });
};

const checkUserActivity = function (ctx) {
    var time = now();
    Object.keys(ctx.users).forEach(function (userId) {
        let u = ctx.users[userId];
        try {
            if (time - u.timeOfLastMessage > LAG_MAX_BEFORE_DISCONNECT) {
                ctx.dropUser(u, "INACTIVITY");
            }
            if (!u.pingOutstanding && time - u.timeOfLastMessage > LAG_MAX_BEFORE_PING) {
                sendMsg(ctx, u, [0, '', 'PING', now()]);
                u.pingOutstanding = true;
            }
        } catch (err) {
            ctx.emit.error(err, 'USER_ACTIVITY_CHECK');
        }
    });
};


module.exports.create = function (socketServer) {
    const Server = {};
    const emit = {};
    const handlers = {};

    [
        'sessionClose',
        'sessionOpen',
        'message',
        'error',          // (err, label, info)
    ].forEach(function (key) {
        const stack = handlers[key] = [];
        emit[key] = function () {
            var l = stack.length;
            for (var i = 0; i < l; i++) {
                stack[i].apply(null, arguments);
            }
        };
    });

    Server.on = function (key, handler) {
        if (!Array.isArray(handlers[key])) {
            return void console.error(new Error("Unsupported event type"));
        }
        if (typeof(handler) !== 'function') {
            return void console.error(new Error("no function supplied"));
        }
        handlers[key].push(handler);
        return Server;
    };

    Server.off = function (key, handler) {
        if (!Array.isArray(handlers[key])) {
            return void console.error(new Error("Unsupported event type"));
        }
        if (typeof(handler) !== 'function') {
            return void console.error(new Error("no function supplied"));
        }
        var index = handlers[key].indexOf(handler);

        if (index < 0) { return; }
        handlers[key].splice(index, 1);
    };

    let ctx = {
        users: {},
        intervals: {},
        emit: emit,
        Server: Server,
        stats: {
            sent: 0,
            sentSize: 0,
            received: 0,
            receivedSize: 0
        }
    };

    Server.send = function (userId, msg, cb) {
        sendMsg(ctx, ctx.users[userId], msg, cb);
    };

    ctx.dropUser = function (user, reason) {
        dropUser(ctx, user, reason);
    };

    ctx.intervals.userActivityInterval = setInterval(function () {
        checkUserActivity(ctx);
    }, 5000);

    var createUniqueName = function () {
        var name = randName();
        if (typeof(ctx.users[name]) === 'undefined') { return name; }
        return createUniqueName();
    };

    socketServer.on('connection', function(socket, req) {
        // refuse new connections if the server is shutting down
        if (!socket.upgradeReq) { socket.upgradeReq = req; }
        let conn = socket.upgradeReq.connection;
        let ip = (req.headers && req.headers['x-real-ip']) || req.socket.remoteAddress || '';
        let user = {
            addr: conn.remoteAddress + '|' + conn.remotePort,
            socket: socket,
            id: createUniqueName(),
            timeOfLastMessage: now(),
            pingOutstanding: false,
            inQueue: 0,
            ip: ip.replace(/^::ffff:/, ''),
            sendMsgCallbacks: []
        };
        ctx.users[user.id] = user;
        sendMsg(ctx, user, [0, '', 'IDENT', user.id]);

        ctx.emit.sessionOpen({id: user.id, ip: user.ip});

        socket.on('message', function(message) {
            try {
                handleMessage(ctx, user, message);
            } catch (e) {
                emit.error(e, 'NETFLUX_BAD_MESSAGE', {
                    user: user.id,
                    message: message,
                });
                ctx.dropUser(user, 'BAD_MESSAGE');
            }
        });
        socket.on('close', function (aa) {
            ctx.dropUser(user, 'SOCKET_CLOSED');
        });
        socket.on('error', function (err) {
            emit.error(err, 'NETFLUX_WEBSOCKET_ERROR');
            ctx.dropUser(user, 'SOCKET_ERROR');
        });
    });

    return Server;
};
