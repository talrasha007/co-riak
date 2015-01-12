var _ = require('codash'),
    RiakProtocol_v2 = require('./riak-2.0.js'),
    request = require('co-request').defaults({ agentOptions: { keepAlive: true } });

var Client = module.exports = function (opt) {
    this._protocol = new RiakProtocol_v2(this); // Riak 2.0 protocol only now.

    var proto = opt.https ? 'https://' : 'http://';
    this._urlPrefixes = _.map(opt.servers || [opt.server], function (svr) {
        return proto + svr;
    });
    this._cnt = 0;
};

Client.prototype = {
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
        return yield request(param);
    }
};
