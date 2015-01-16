var _ = require('codash'),
    path = require('path'),
    fs = require('fs'),
    PbSchema = require('protobuf').Schema;

var riakDtPb = new PbSchema(fs.readFileSync(path.join(__dirname, 'proto', 'riak_dt.desc'))),
    riakSearchPb = new PbSchema(fs.readFileSync(path.join(__dirname, 'proto', 'riak_search.desc'))),
    riakKvPb = new PbSchema( fs.readFileSync(path.join(__dirname, 'proto', 'riak_kv.desc')) );

var RiakPbProtocol_v2 = module.exports = function (client) {
    this._client = client;
};

RiakPbProtocol_v2.prototype = {
    save: function *(type, bucket, key, value, meta, opt) {
        var pb = pbSchemas.RpbPutReq.serialize({
            bucket: bucket,
            key: key,
            content: {
                value: Buffer.isBuffer(value) || _.isString(value) ? value : JSON.stringify(value),
                contentType: Buffer.isBuffer(value) ? 'application/octet-stream' :  _.isString(value) ? 'text/plain' : 'application/json',
                indexes: _.reduce(meta.index, function (m, idx, key) {
                    idx.forEach(function (v) {
                        m.push({ key: key + (_.isNumber(v) ? '_int' : '_bin'), value: v });
                    });
                    return m;
                }, [])
            },
            type: type
        });

        var res = yield* this._client._exec(messageCodes.RpbPutReq, pb);
        if (res.mc !== messageCodes.RpbErrorResp) {
            return pbSchemas[res.mc].parse(res.data);
        } else {
            console.log(pbSchemas[res.mc].parse(res.data).errmsg.toString());
        }
    },

    get: function *(type, bucket, key, opt) {
        var pb = pbSchemas.RpbGetReq.serialize({
            bucket: bucket,
            key: key,
            type: type
        });

        var res = yield* this._client._exec(messageCodes.RpbGetReq, pb);
        if (res.mc !== messageCodes.RpbErrorResp) {
            return parseRpbContent(pbSchemas[res.mc].parse(res.data).content[0]);
        }
    },

    /* Query 2i */
    query: function *(type, bucket, idxName, idx1, idx2, opt) {
        var isRange = typeof idx1 === typeof idx2,
            query = {
                bucket: bucket,
                index: idxName + (_.isNumber(idx1) ? '_int': '_bin'),
                qtype: isRange ? 1 : 0,
                type: type
            };

        if (isRange) {
            query.range_min = idx1;
            query.range_max = idx2;
        } else {
            query.key = idx1;
        }

        var pb = pbSchemas.RpbIndexReq.serialize(query);

        var res = yield* this._client._exec(messageCodes.RpbIndexReq, pb);
        if (res.mc !== messageCodes.RpbErrorResp) {
            return {
                data: pbSchemas[res.mc].parse(res.data).keys
            };
        }
    }
};

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

var pbSchemas = _.reduce(messageCodes, function (m, v, k) {
    m[k] = riakKvPb[k] || riakSearchPb[k] || riakDtPb[k];
    m[v] = m[k];
    return m;
}, {});

var indexRegex = /^(.*)_(int|bin)$/;
function parseRpbContent(ct) {
    var contentType = ct.contentType.toString(),
        value = contentType === 'application/json' ? JSON.parse(ct.value.toString()) :
            contentType === 'text/plain' ? ct.value.toString() : ct.value;

    return {
        data: value,
        meta: {
            contentType: contentType,
            vtag: ct.vtag.toString(),
            lastMod: new Date(ct.lastMod * 1000),
            index: _.reduce(ct.indexes, function (r, idx) {
                var m = indexRegex.exec(idx.key.toString()),
                    key = m[1],
                    type = m[2],
                    value = type === 'int' ? parseInt(idx.value.toString()) : idx.value;


                if (!r[key]) r[key] = [value];
                else r[key].push(value);

                return r;
            }, {})
        }
    };
}
