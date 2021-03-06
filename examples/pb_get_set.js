var co = require('co'),
    db = require('../').getClient({ servers: ['10.10.35.159:8087'], proto: 'pb' });

co(function *() {
    var devices = db.bucket('dmp', 'devices');

    yield* devices.new('0010', { imei: ['859000000001'], idfa: ['ABCDE'] }).index({ foo: [1, 2], imei: ['859000000001'], idfa: ['ABCDE', 'FDE'] }).save();

    console.log(yield* devices.query('imei', '859000000001'));
    console.log(yield* devices.query('idfa', 'FDE'));

    var item = yield* devices.get('0010');
    console.log(item.key());
    console.log(item.val());
    console.log(item.meta());
    yield* item.index({ foo: [2, 3] }, true).save();
    var item = yield* devices.get('0010');
    console.log(item.key());
    console.log(item.val());
    console.log(item.meta());

    yield* devices.new(new Buffer('00AA', 'hex'), new Buffer('AABBCC', 'hex')).index({ foo: [new Buffer('CCDD', 'hex')] }).save();
    item = yield* devices.get(new Buffer('00AA', 'hex'));
    console.log(item.key());
    console.log(item.val());
    console.log(item.meta());
}).then(function () {
    console.log('done...');
    process.exit(0);
}, function (err) {
    console.error(err.stack);
    process.exit(0);
});
