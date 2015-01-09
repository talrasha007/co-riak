var Client = require('./lib/client.js');

exports.getClient = function (opt) {
    return new Client(opt);
};
