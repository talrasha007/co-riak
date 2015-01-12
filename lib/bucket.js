var DbObject = require('./object.js');

var Bucket = module.exports = function (client, type, name) {
    this._type = type;
    this._name = name;
    this._protocol = client._protocol;
};

Bucket.prototype = {
    new: function (key, obj) {
        return new DbObject(this, key, obj);
    },

    get: function *(key) {
        var res = yield* this._protocol.get(this._type, this._name, key);
        if (res.statusCode === 200) return new DbObject(this, key, res.body, res.meta);
    },

    query: function *(idxName, idx1, idx2) {
        return yield* this._protocol.query(this._type, this._name, idxName, idx1, idx2);
    }
};