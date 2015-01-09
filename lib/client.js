var _ = require('codash'),
    request = require('co-request').defaults({ agentOptions: { keepAlive: true } });

var Client = module.exports = function (opt) {
    this._protocol = require('./riak-2.0.js'); // Riak 2.0 protocol only now.

    var proto = opt.https ? 'https://' : 'http://';
    this._urlPrefixes = _.map(opt.servers || [opt.server], function (svr) {
        return proto + svr;
    });
};

Client.prototype = {
    _get: function *() {

    },

    _put: function *() {

    },

    _del: function *() {

    }
};
