var co = require('co'),
    db = require('../').getClient({ servers: ['10.10.35.159:8098'] });

co(function *() {
    var devices = db.bucket('dmp', 'devices');

    yield* devices.new('0010', { imei: ['859000000001'], idfa: ['ABCDE'] }).index({ imei: ['859000000001'], idfa: ['ABCDE', 'FDE'] }).save();

    console.log(yield* devices.get('0010'));
    console.log(yield* devices.query('imei', '859000000001'));
    console.log(yield* devices.query('idfa', 'FDE'));
}).catch(function (err) {
    console.error(err.stack);
});
