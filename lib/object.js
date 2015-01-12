var _ = require('codash');

var DbObject = module.exports = function (bucket, key, value, meta) {
    this._bucket = bucket;
    this._key = key;
    this._value = value;
    this._meta = meta || {};
};

DbObject.prototype = {
    key: function () {
        return this._key;
    },

    val: function (v) {
        if (v) this._value = v;
        else return this._value;
    },

    meta: function (m) {
        if (m) this._meta = m;
        else return this._meta;
    },

    index: function (indexes) {
        var me = this;
        if (!this._meta.index) this._meta.index = {};

        _.each(indexes, function (idx, name) {
            if (!_.isArray(idx)) idx = [idx];

            var originIdx = me._meta.index[name] || [];
            me._meta.index[name] = _.union(originIdx, idx);
        });

        return this;
    },

    save: function *(opt) {
        var bucket = this._bucket,
            headers = {};

        _.each(this._meta.index, function (idx, name) {
            if (idx.length > 0) {
                var field = 'x-riak-index-' + name + (_.isNumber(idx[0]) ? '_int' : '_bin');
                headers[field] = idx;
            }
        });

        yield* bucket._protocol.save(bucket._type, bucket._name, this._key, this._value, headers, opt);
    }
};
