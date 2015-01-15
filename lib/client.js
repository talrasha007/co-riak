var _ = require('codash'),
    Bucket = require('./bucket.js'),
    RiakHttpProtocol_v2 = require('./riak.http-2.0.js'),
    request = require('co-request').defaults({ agentOptions: { keepAlive: true } });

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
