var expect = require('expect.js'),
    co = require('co'),
    db = require('../').getClient({ servers: ['10.10.35.159:8098'], proto: 'http' }),
    bucket = db.bucket('default', '_test_');

describe('Basic Operations', function () {
    it('should get same item just putted.', function (cb) {
        co(function *() {
            yield* bucket.new('0010', { imei: ['859000000001'], idfa: ['ABCDE'] }).index({ foo: [1, 2], imei: ['859000000001'], idfa: ['ABCDE', 'FDE'] }).save();

            var q1 = yield* bucket.query('imei', '859000000001');
            var q2 = yield* bucket.query('idfa', 'FDE');

            expect(q1.body.keys).to.eql(['0010']);
            expect(q2.body.keys).to.eql(['0010']);

            var item = yield* bucket.get('0010');
            expect(item.key()).to.eql('0010');
            expect(item.val()).to.eql({ imei: ['859000000001'], idfa: ['ABCDE'] });
            expect(item.meta().index).to.eql({ foo: [1, 2], imei: ['859000000001'], idfa: ['ABCDE', 'FDE'] });
        }).then(cb, cb);
    });
});