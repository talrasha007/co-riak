var _ = require('codash');

var RiakHttpProtocol_v2 = module.exports = function (client) {
    this._client = client;
};

RiakHttpProtocol_v2.prototype = {
    save: function *(type, bucket, key, value, header, opt) {
        header = header || {};

        if (!_.isString(value)) {
            value = JSON.stringify(value);
            header['Content-Type'] = 'application/json';
        } else {
            header['Content-Type'] = 'text/plain';
        }

        return yield* this._client._put(['/types', type, 'buckets', bucket, 'keys', key].join('/'), opt, header, value);
    },

    get: function *(type, bucket, key, opt) {
        var res = yield* this._client._get(['/types', type, 'buckets', bucket, 'keys', key].join('/'), opt);
        if (res.statusCode === 200) {
            return {
                data: res.body,
                meta: res.meta
            };
        }
    },

    /* Query 2i */
    query: function *(type, bucket, idxName, idx1, idx2, opt) {
        var field = [idxName, _.isNumber(idx1) ? 'int' : 'bin'].join('_'),
            path = ['/types', type, 'buckets', bucket, 'index', field, idx1];

        if (typeof idx1 === typeof idx2) path.push(idx2);

        var res = yield* this._client._get(path.join('/'), opt);
        if (res.statusCode === 200) {
            return {
                data: res.body.keys
            };
        }
    }
};