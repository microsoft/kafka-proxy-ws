'use strict';

const should = require('should'),
    debug = require('debug')('kafka-prox-ws-test');

let defaults = {
        port : 9999,
        kafkaAddress : 'localhost:9092/',
        idleTimeout : 1000,
        maxBytes : 500000,
        recordInterval: 5000,
        defaultPartition: 0,
        auth: null
}

describe('kafka proxy unit tests', () => {
    it('create constructor with options', (done) => {        
        var KafkaProxy = require('../index');

        let kafkaProxy = new KafkaProxy({
            wsPort: defaults.port, 
            kafka: defaults.kafkaAddress,
            idleTimeout: defaults.idleTimeout + 1,
            recordInterval: defaults.recordInterval + 1,
            maxBytes: defaults.maxBytes + 1,
            partition: defaults.defaultPartition + 1,
            auth: 'password'    
        });

        kafkaProxy.port.should.equal(defaults.port);
        kafkaProxy.kafkaAddress.should.equal(defaults.kafkaAddress);
        kafkaProxy.idleTimeout.should.equal(defaults.idleTimeout + 1);
        kafkaProxy.maxBytes.should.equal(defaults.maxBytes + 1);
        kafkaProxy.recordInterval.should.equal(defaults.recordInterval + 1);
        kafkaProxy.defaultPartition.should.equal(defaults.defaultPartition + 1);
        kafkaProxy.auth.should.equal('password');
        done();
    });

    it('create constructor without options and check defaults are set', (done) => {        
        var KafkaProxy = require('../index');

        let kafkaProxy = new KafkaProxy({
            wsPort: defaults.port, 
            kafka: defaults.kafkaAddress,
        });

        kafkaProxy.port.should.equal(defaults.port);
        kafkaProxy.kafkaAddress.should.equal(defaults.kafkaAddress);
        kafkaProxy.idleTimeout.should.equal(defaults.idleTimeout);
        kafkaProxy.maxBytes.should.equal(defaults.maxBytes);
        kafkaProxy.recordInterval.should.equal(defaults.recordInterval);
        kafkaProxy.defaultPartition.should.equal(defaults.defaultPartition);
        should.not.exist(kafkaProxy.auth);
        done();
    });


    it('create constructor without an address should throw an error', (done) => {        
        var KafkaProxy = require('../index');

        try {
            let kafkaProxy = new KafkaProxy({});
        } catch (ex) {
            should.exist(ex);
        }
        done();
    });

});
