var co = require('co'),
    db = require('../').getClient({ servers: ['10.10.35.159:8098'] });

co(function *() {
    var devices = db.bucket('devices');

    yield* devices.new('00100', { imei: ['859000000001'], idfa: ['ABCDE'] }).index({ imei: ['859000000001'], idfa: ['ABCDE'] }).save();
    console.log(yield* devices.get('00100'));
    console.log(yield* devices.query({ imei: '859000000001' }));
}).catch(function (err) {
    console.error(err.stack);
});
