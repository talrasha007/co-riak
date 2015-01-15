var _ = require('codash'),
    net = require('net'),
    Bucket = require('./bucket.js'),
    RiakHttpProtocol_v2 = require('./riak.http-2.0.js'),
    RiakPbProtocol_v2 = require('./riak.pb-2.0.js'),
    request = require('co-request').defaults({ agentOptions: { keepAlive: true } });

function PBConnection(ip, port) {
    this._client = null;
    this._ready = false;
    this._cb = null;

    this._reconnect(ip, port);
}

PBConnection.prototype = {
    exec: function *(mc, pb) {
        var me = this;

        while (!this._ready || this._cb) yield _.sleep(0);

        var len = (pb && pb.length) || 0,
            buf = new Buffer(len + 5);

        buf.writeUInt32LE(len + 1, 0);
        buf.writeUint8(mc, 4);
        if (pb) pb.copy(buf, 5);
        this._client.write(buf);

        return yield function (cb) {
            me._cb = cb;
        };
    },

    _reconnect: function (ip, port) {
        var me = this;

        this._client = net.connect({ port: port, host: ip }, function () {
            me._ready = true;
        });

        var buf;
        this._client.on('data', function (data) {
            buf = buf ? Buffer.concat([buf, data]) : data;

            var len = buf.readUInt32LE(0);
            if (len + 5 === buf.length) {
                var cb = me._cb;
                me._cb = null;
                cb({
                    mc: buf.readUint8(4),
                    data: buf.slice(5)
                });
            }
        });

        this._client.on('error', function (err) {
            me._ready = false;

            console.error(ip, ':', port, 'error:', err.code);
            if (me._cb) {
                var cb = me._cb;
                me._cb = null;
                cb(err);
            }

            me._reconnect(ip, port);
        });
    }
};

var PBClient = exports.PBClient = function (opt) {
    this._protocol = new RiakPbProtocol_v2(this);

    this._connections = [];
    for (var i = 0; i < opt.maxConnection || 16; i++) {
        var pp = opt.servers[i % opt.servers.length].split(':'),
            ip = pp[0],
            port = pp[1];

        this._connections.push(new PBConnection(ip, port));
    }

    this._cnt = 0;
};

PBClient.prototype = {
    bucket: function (type, name) {
        return new Bucket(this, type, name);
    },

    _exec: function *(messageCode, buffer) {
        var conn = this._connections[this._cnt++ % this._connections.length];
        return yield* conn.exec(messageCode, buffer);
    }
};

var HttpClient = exports.HttpClient = function (opt) {
    this._protocol = new RiakHttpProtocol_v2(this); // Riak 2.0 protocol only now.

    this._urlPrefixes = _.map(opt.servers || [opt.server], function (svr) {
        return (opt.proto || opt.protocol) + '://' + svr;
    });
    this._cnt = 0;
};

HttpClient.prototype = {
    bucket: function (type, name) {
        return new Bucket(this, type, name);
    },

    _get: function *(path, query, header) {
        return yield* this._request({
            url: path,
            method: 'GET',
            qs: query,
            headers: header
        });
    },

    _put: function *(path, query, header, body) {
        return yield* this._request({
            url: path,
            method: 'PUT',
            qs: query,
            headers: header,
            body: body
        });
    },

    _del: function *(path, query, header) {
        return yield* this._request({
            url: path,
            method: 'DELETE',
            qs: query,
            headers: header
        });
    },

    _request: function *(param) {
        var idx = (this._cnt++ % this._urlPrefixes.length);
        param.url = this._urlPrefixes[idx] + param.url;

        var res = yield request(param);
        if (res.body && res.headers['content-type'] === 'application/json') res.body = JSON.parse(res.body);

        return {
            statusCode: res.statusCode,
            body: res.body,
            meta: res.headers
        };
    }
};
