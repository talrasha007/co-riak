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
        return yield* this._protocol.get(this._type, this._name, key);
    },

    query: function *(idxName, idx1, idx2) {
        return yield* this._protocol.query(this._type, this._name, idxName, idx1, idx2);
    }
};