var _ = require('codash'),
    path = require('path'),
    fs = require('fs'),
    PbSchema = require('protobuf').Schema;

var riakDtPb = new PbSchema(fs.readFileSync(path.join(__dirname, 'proto', 'riak_dt.desc'))),
    riakKvPb = new PbSchema( fs.readFileSync(path.join(__dirname, 'proto', 'riak_kv.desc')) ),
    riakSearchPb = new PbSchema(fs.readFileSync(path.join(__dirname, 'proto', 'riak_search.desc')));

var RiakPbProtocol_v2 = module.exports = function (client) {
    this._client = client;
};

RiakPbProtocol_v2.prototype = {
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
        var pb =RpbGetReq.serialize({
            bucket: bucket,
            key: key,
            type: type
        });

        return yield* this._client._exec(messageCodes.RpbGetReq, pb);
    },

    /* Query 2i */
    query: function *(type, bucket, idxName, idx1, idx2, opt) {
        var field = [idxName, _.isNumber(idx1) ? 'int' : 'bin'].join('_'),
            path = ['/types', type, 'buckets', bucket, 'index', field, idx1];

        if (typeof idx1 === typeof idx2) path.push(idx2);

        return yield* this._client._get(path.join('/'), opt);
    }
};

var RpbGetReq = riakKvPb['RpbGetReq'];

var messageCodes = {
    RpbErrorResp             : 0,
    RpbPingReq               : 1,
    RpbPingResp              : 2,
    RpbGetClientIdReq        : 3,
    RpbGetClientIdResp       : 4,
    RpbSetClientIdReq        : 5,
    RpbSetClientIdResp       : 6,
    RpbGetServerInfoReq      : 7,
    RpbGetServerInfoResp     : 8,
    RpbGetReq                : 9,
    RpbGetResp               : 10,
    RpbPutReq                : 11,
    RpbPutResp               : 12,
    RpbDelReq                : 13,
    RpbDelResp               : 14,
    RpbListBucketsReq        : 15,
    RpbListBucketsResp       : 16,
    RpbListKeysReq           : 17,
    RpbListKeysResp          : 18,
    RpbGetBucketReq          : 19,
    RpbGetBucketResp         : 20,
    RpbSetBucketReq          : 21,
    RpbSetBucketResp         : 22,
    RpbMapRedReq             : 23,
    RpbMapRedResp            : 24,
    RpbIndexReq              : 25,
    RpbIndexResp             : 26,
    RpbSearchQueryReq        : 27,
    RbpSearchQueryResp       : 28,
    RpbResetBucketReq        : 29,
    RpbResetBucketResp       : 30,
    RpbGetBucketTypeReq      : 31,
    RpbSetBucketTypeResp     : 32,
    RpbCSBucketReq           : 40,
    RpbCSUpdateReq           : 41,
    RpbCounterUpdateReq      : 50,
    RpbCounterUpdateResp     : 51,
    RpbCounterGetReq         : 52,
    RpbCounterGetResp        : 53,
    RpbYokozunaIndexGetReq   : 54,
    RpbYokozunaIndexGetResp  : 55,
    RpbYokozunaIndexPutReq   : 56,
    RpbYokozunaIndexPutResp  : 57,
    RpbYokozunaSchemaGetReq  : 58,
    RpbYokozunaSchemaGetResp : 59,
    RpbYokozunaSchemaPutReq  : 60,
    DtFetchReq               : 80,
    DtFetchResp              : 81,
    DtUpdateReq              : 82,
    DtUpdateResp             : 83,
    RpbAuthReq               : 253,
    RpbAuthResp              : 254,
    RpbStartTls              : 255
};