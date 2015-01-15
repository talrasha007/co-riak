var clients = require('./lib/client.js');

exports.getClient = function (opt) {
    switch (opt.proto || opt.protocol) {
        case 'http':
        case 'https':
            return new (clients.HttpClient)(opt);
        default :
            return new (clients.PBClient)(opt);
    }
};
