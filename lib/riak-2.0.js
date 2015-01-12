var _ = require('codash');

var RiakProtocol_v2 = function (client) {
    this._client = client;
};

RiakProtocol_v2.prototype = {
    save: function *(type, bucket, key, value, header, opt) {
        header = header || {};

        if (!_.isString(value)) {
            value = JSON.stringify(value);
            header.contentType = 'application/json';
        } else {
            header.contentType = 'text/plain';
        }

        return yield* this._client._put([].join('/'), opt, header, value);
    },

    get: function *(type, bucket, key, opt) {
        return yield* this._client._get(['/types', type, 'buckets', bucket, 'keys', key].join('/'), opt);
    },

    /* Query 2i */
    query: function *(type, bucket, idxName, idx1, idx2, opt) {
        var field = [idxName, _.isNumber(idx1) ? 'int' : 'bin'].join('_'),
            path = ['/types', type, 'buckets', bucket, 'index', field, idx1];

        if (typeof idx1 === typeof idx2) path.push(idx2);

        return yield* this._client._get(path.join('/'), opt);
    }
};