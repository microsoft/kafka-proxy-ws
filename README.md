#kafka-proxy

A robust, scalable, high performance WebSockets based proxy for Kafka. 

```
'use strict';
const KafkaProxy = require('kafka-proxy');

let kafkaProxy = new KafkaProxy({
    wsPort: 9999, 
    kafka: 'localhost:9092/',
});

kafkaProxy.listen();
```

## Why a proxy for Kafka? 
This library adds a few features that Kafka itself doesn’t natively support such as easy connectivity to Kafka over standard web protocols and a central point of management for offsetting, logging, alerting. These capabilities aim to increase agility while in development and can also prove useful in production.    

##Features
* Enables connectivity to Kafka via WebSockets, leveraging all their benefits such as performance, security, cross-platform, etc. For example, runs over HTTP(S), making it easy to connect to Kafka through a firewall without having to expose the broker address / ports 
* Any standard web socket library should work (we test / runn with the excellent [ws](https://www.npmjs.com/package/ws) library in production)
* Multi tenant. A single proxy can handle many incoming clients / web sockets connections. Client uniqueness is maintained via the topic / consumer group / partition combo. 
* Auto offset management. Connect to kafka-proxy by either specifying an offset, or optionally letting the proxy manage the offset for you (recommended for development only at this time - more details below).
* Centralized reporting 
* Stable. Observed to be running in production for weeks on end without a dropped web socket connection, processing 10M’s of messages
* High performance. Tested locally on a quad core PC at 30k+ messages / second
* Built on top of the excellent [no-kafka](https://www.npmjs.com/package/no-kafka) library (enabling connection directly to Kafka brokers), so kafka-proxy inherits the ability to set throughput rate (e.g. # of bytes per batch of messages, delay between messages). 

##Usage
### Server
First, create a server which will connect to your Kafka broker(s) and also listening for any incoming web socket connections:

```
'use strict';
const KafkaProxy = require('kafka-proxy');

let kafkaProxy = new KafkaProxy({
    wsPort: 9999, 
    kafka: 'localhost:9092/',
});

kafkaProxy.listen();
```

### Consuming messages

Then create a web socket client to listen for messages on the topic. This can be done easily through:

1. Your own WebSocket client
2. Using the included consumer.js file in the ./samples directory
3. By installing the [wscat](https://www.npmjs.com/package/wscat) client

This is an example wscat connection string:

```
wscat --connect "ws://127.0.0.1:9999/?topic=test&consumerGroup=group1"
```
That's it ! Now whenever messages are sent to your Kafka broker for the topic "test", you'll receive them over this WebSocket.

An optional, but recommended, parameter that can be sent over the WebSocket URL is 'offset'. Kafka-Proxy will automatically maintain an offset for you, but there are cases where it can skip forwards (e.g. if your process crashes during receiving a batch, the whole batch can be marked as read). If you need accurate offset management, best results will experienced by maintaining your own offset and passing it into the URL each time. For example: 

```
wscat --connect "ws://127.0.0.1:9999/?topic=test&consumerGroup=group1&offset=1000"
```

The file ./samples/consumer.js shows an example of managing an offset locally by storing it in a file. Another good option is redis.

### Message format
Messages are received in batches (according to the set batch size) over the WebSocket in the follolowing format:
```
[
    {"message":"hello one","offset":225107},
    {"message":"hello two","offset":225108},
    {"message":"hello three","offset":225109}
]
```

### Startup Options
kafka-proxy can be constructed with the following optional parameters:
```
let kafkaProxy = new KafkaProxy({
    wsPort: 9999, // required
    kafka: 'localhost:9092/', // required
    idleTimeout: 100, // time to wait between batches
    maxBytes: 1000000, // the max size of a batch to be downloaded
    partition: 0, // the default partition to listen to
    auth: 'thisisapassword' // optional authentication
});
```

### Authentication
Baic HTTP authentication can be enabled by setting the "auth" parameter in the constructor. After this is set, it can be sent over the WebSocket. E.g.
```
wscat --connect "ws://127.0.0.1:9999/?topic=test&consumerGroup=group1" -H 'authorization: basic thisisapassword'
```


## Limitations
This is an early project. I started a new, clean repo as the old one had a long and unnecessary commit history. It's a stable code base and we have this running in production for several months. A couple of notes / limitations:
* This proxy is for receiving messages only. No sending capability yet. Our scenarios mostly have required getting messages off of Kafka to dev machines (rather than sending them back in directly). If there’s demand, I’ll add a sending capability too. 
* Make sure you set an appropriate throughput rate (using the **maxBytes** and **idleTimeout** variables) to avoid “back pressure”. If set higher than your client’s ability to process messages. kafka-proxy can send too quickly and crash your client by out of memory.  
* Auto offsetting. This is a useful feature but there are cases where messages can be skipped (e.g. if your process crashes halfway through receiving a batch).  
* If a batch of messages is sent greater in size than maxBytes, messages will not be consumable until maxBytes is set above this value. Needs investigation as to how to solve programmatically.

## Future features
Planned future features:
* Full test suite and CLI tools
* Sending capability (current limited to receiving only)
* Improved auto offset management 
* More robust handling of maxBytes e.g. via a warning / error message when exceeded
