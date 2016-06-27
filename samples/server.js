'use strict';

const KafkaProxy = require('../index');

console.log('Starting Web Socket Server');

// initiate the proxy
let kafkaProxy = new KafkaProxy({
    wsPort: 9999, 
    kafka: 'localhost:9092/',
    idleTimeout: 100,
    maxBytes: 1000000,
    partition: 0,    
    //auth: 'thisisapassword'
});

kafkaProxy.listen();

console.log('Started Web Socket Server');

// Handle uncaught exceptions

if(typeof v8debug !== 'object') {
    console.log('No debugger attached, listening for uncaught exceptions for logging...');
     
    process.on('uncaughtException', function(err) {
        console.error('UNCAUGHT EXCEPTION!')
        console.error((new Date).toUTCString() + ' uncaughtException:', err.message);
        console.error(err.stack);

        // Exit the app on the next tick, incase others are listen to uncaughtException
        process.nextTick(function() {
            process.exit(1);
        });
    });
}
