
var DbObject = module.exports = function (bucket, key, value, meta, modified) {
    this._modified = !!modified;
    this._bucket = bucket;
    this._key = key;
    this._value = value;
    this._meta = meta || {};
};

DbObject.prototype = {
    key: function () {
        return this._key;
    },

    val: function () {
        return this._value;
    },

    index: function () {
        return this;
    },

    save: function *(opt) {
        var bucket = this._bucket,
            headers = {};

        yield* bucket._protocol.save(bucket._type, bucket._name, this._key, this._value, headers, opt);
    }
};