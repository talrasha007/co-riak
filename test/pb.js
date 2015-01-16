var expect = require('expect.js'),
    co = require('co'),
    db = require('../').getClient({ servers: ['10.10.35.159:8087'] }),
    bucket = db.bucket('default', '_test_');

describe('PBC Basic Operations', function () {
    it('should get same item just putted.', function (cb) {
        co(function *() {
            yield* bucket.new('0010', { imei: ['859000000001'], idfa: ['ABCDE'] }).index({ foo: [1, 2], imei: ['859000000001'], idfa: ['ABCDE', 'FDE'] }).save();

            var q1 = yield* bucket.query('imei', '859000000001');
            var q2 = yield* bucket.query('idfa', 'FDE');

            expect(q1.data).to.eql([new Buffer('0010')]);
            expect(q2.data).to.eql([new Buffer('0010')]);

            var item = yield* bucket.get('0010');
            expect(item.key()).to.eql('0010');
            expect(item.val()).to.eql({ imei: ['859000000001'], idfa: ['ABCDE'] });
            expect(item.meta().index).to.eql({ foo: [1, 2], imei: [new Buffer('859000000001')], idfa: [new Buffer('ABCDE'), new Buffer('FDE')] });
        }).then(cb, cb);
    });

    it('binary data should be ok', function (cb) {
        co(function *() {
            yield* bucket.new(new Buffer('00AA', 'hex'), new Buffer('AABBCC', 'hex')).index({ foo: [new Buffer('CCDD', 'hex')] }).save();
            var item = yield* bucket.get(new Buffer('00AA', 'hex'));
            expect(item.key()).to.eql(new Buffer('00AA', 'hex'));
            expect(item.val()).to.eql(new Buffer('AABBCC', 'hex'));
            expect(item.meta().index).to.eql({ foo: [new Buffer('CCDD', 'hex')] });
        }).then(cb, cb);
    })
});