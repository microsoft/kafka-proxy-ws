'use strict';

const nokafka = require('no-kafka'),
    program = require('commander');

let producer;

program
  .option('-t, --topic <value>', 'topic (required)')
  .option('-n, --num [value]', 'number of messages or batches', 100)
  .option('-p, --partition [value]', 'option partition (default is 0)')
  .parse(process.argv);

let topic = program.topic;
let numMessages = Number.parseInt(program.num);
let partition = program.partition;

if (!topic) {
    program.outputHelp();
    process.exit(1);
} 

function connectToKafka() {
    // connect to kafka
    console.log("CONNECTING TO KAFKA")
    
    // Create producer 
    producer = new nokafka.Producer({
        connectionString: 'localhost:9092',
        clientId: 'myservice',
        codec: nokafka.COMPRESSION_SNAPPY
    });

    // connect to kafka
    return producer.init().then(() => {
        
        // create a batch of messages and send to kafka
        let messages = [];
        for (var i=0; i<numMessages;i++) {
            messages.push(
                {
                    fixedString: 'hello world',
                    randomNumber: Math.random(),
                    randomString: Math.random().toString(),
                    count: (i+1).toString(),
                    object: {
                        'something': 'else'
                    }
                })
        }
        
        // send the batch
        sendToKafka({messages:messages}, topic, 0);
    });
}


function sendToKafka(messages, queueName, partitionId) {
    // create kafka messages    
    let kafkaMsgs = [];
    for (var i = 0; i < messages.messages.length; i++) {
        kafkaMsgs.push({
            topic: queueName,
            message: { value: JSON.stringify(messages.messages[i]) },
            partition: partitionId
        });
    };

    console.log(`sending ${kafkaMsgs.length} messages to KAFKA TOPIC ${queueName}, partitionId ${partitionId}`);
    producer.send(kafkaMsgs, {
        batch: {
            size: 1000,
            maxWait: 100
        }
    }).then((data) => {
        console.log(`sent ${kafkaMsgs.length} messages to KAFKA TOPIC ${queueName}, partitionId ${partitionId}. Data from topic is ${JSON.stringify(data)}`);
        process.exit(1);
    }).
        catch((err) => {
            console.log('err on kafka queue.push: ' + err);
        });
}

connectToKafka();
