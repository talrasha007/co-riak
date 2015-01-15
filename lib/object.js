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

    index: function (indexes, overwrite) {
        var me = this;
        if (!this._meta.index || overwrite) this._meta.index = {};

        _.each(indexes, function (idx, name) {
            if (!_.isArray(idx)) idx = [idx];

            if (overwrite) {
                me._meta.index[name] = idx;
            } else {
                var originIdx = me._meta.index[name] || [];
                me._meta.index[name] = _.union(originIdx, idx);
            }
        });

        return this;
    },

    save: function *(opt) {
        var bucket = this._bucket;
        yield* bucket._protocol.save(bucket._type, bucket._name, this._key, this._value, this._meta, opt);
    }
};
