'use strict';

const kafka = require('no-kafka'),
    debug = require('debug')('kafka-ws-proxy'),
    querystring = require('querystring'),
    WebSocket = require('ws');

let errorCount = 0;

class KafkaProxy {

    // constructor
    constructor(config) {
        // web socket stuff
        this.WebSocketServer = {};
        this.wss = {};
        this.ws = {};
        if (!config.wsPort) {
            throw ("web socket port needed, please set e.g. {wsPort: 9999}");
        }
        this.port = config.wsPort;

        if (!config.kafka) {
            throw ("kafka server address needed, please set e.g. {kafka: \"localhost:2181/\"}");
        }
        
        // tracking variables 
        this.clients = {}
        this.offsets = {};
        this.consumers = {};
        
        // config and default settings
        this.kafkaAddress = config.kafka;
        this.idleTimeout = Number.parseInt(config.idleTimeout || 1000);
        this.maxBytes = Number.parseInt(config.maxBytes || 500000);
        this.recordInterval = Number.parseInt(config.recordInterval || 5000);
        this.defaultPartition = config.partition || 0;
        this.auth = config.auth;
    }

    listen() {
        // setup websocket server
        this.WebSocketServer = WebSocket.Server,
        this.wss = new this.WebSocketServer({ port: this.port, verifyClient: (info, callback) => this._verifyClient(info, callback)});
        debug('listening on web socket');
        
        // once we have a web socket client connected
        this.wss.on('connection', (ws) => {
            this._registerTopic(ws);
        });

        // error handling
        this.wss.on('error', (err) => {
            debug('something went wrong: ' + err);
        });

        setInterval(() => {
            this._recordMetrics();
        }, this.recordInterval);
    }
    
    // Connect to kafka server and topics
    _connectToKafka(consumerGroup, topic) {
        debug("Connecting to Kafka")
        this.consumers[consumerGroup][topic] = new kafka.SimpleConsumer(
            {
                'connectionString': this.kafkaAddress,
                'idleTimeout': this.idleTimeout,
                'maxBytes': this.maxBytes,
                groupId: consumerGroup
            });

        return this.consumers[consumerGroup][topic].init();
    }

    _registerTopic(ws) {
        debug(`got new incoming web socket connection: ${ws.upgradeReq.url}`);
        // store this web socket connection 
        if (ws.upgradeReq.url) {
            // querystrings always come in like this: /?param1=value1&param2=value2
            let query = querystring.parse(ws.upgradeReq.url.slice(2));
            let topic = query.topic;
            let requestOffset = Number.parseInt(query.offset);
            let consumerGroup = query.consumerGroup || 'default';
            let partition = Number.parseInt(query.partition || this.defaultPartition); 
            
            // tracking variables
            if (!this.clients[consumerGroup]) this.clients[consumerGroup] = {}; 
            if (!this.offsets[consumerGroup]) this.offsets[consumerGroup] = {}; 
            if (!this.consumers[consumerGroup]) this.consumers[consumerGroup] = {}; 

            // if a client for this topic is already connected, kill it
            if (this.clients[consumerGroup][topic]) {
                debug(`client already connected to ${consumerGroup} / ${topic}. disconnecting`);
                this._disconnectClient(consumerGroup, topic, partition, () => {
                    // wait for the kill to complete, then start listening
                    this._startListening(ws, consumerGroup, topic, partition, requestOffset);
                });
            }
            else {
                // if no existing client, start straight away
                this._startListening(ws, consumerGroup, topic, partition, requestOffset);
            }
        }
    }

    _startListening(ws, consumerGroup, topic, partition, requestOffset) {
        // Add the client to tracking tables 
        this.clients[consumerGroup][topic] = this._clientFromWebsocket(ws, consumerGroup, topic);
        this.clients[consumerGroup][topic].ws.on('close', () => {
            debug(`close socket event raised for ${consumerGroup} / ${topic}`);
            this._disconnectClient(consumerGroup, topic, partition, () => {                 
                debug(`socket is closed and cleanup completed for ${consumerGroup} / ${topic}`);
            });
        });
        
        // save the partition this is no (reserved for future)
        this.clients[consumerGroup][topic].partition = partition;

        debug(`[${consumerGroup} / ${topic}] storing incoming connection, querying offset`);
        // if topic is not on this subscription
        if (!this.consumers[consumerGroup][topic]) {
            // reconnect to kafka to get the latest topics
            this._connectToKafka(consumerGroup, topic).then(() => {
                debug('registered consumer to kafka!!!!');
                return this._subscribe(requestOffset, consumerGroup, topic, partition);
            }).catch((ex) => {
                // if we get an error on connecting, delete and return
                debug(`error subscribing to kafka topic. error: ${ex}`);
                return this._disconnectClient(consumerGroup, topic, partition, () => {
                    // delete the consumer so we can try again next time
                    delete this.consumers[consumerGroup][topic];
                    return;
                });
            });
        }
        else {
            return this._subscribe(requestOffset, consumerGroup, topic, partition);
        }
    }

    _subscribe(requestOffset, consumerGroup, topic, partition) {
        if (!requestOffset) {
            // if no offset has been supplied by the client, get the offset from kafka
            return this._getOffset(consumerGroup, topic, partition).then((result) =>{ 
                if (result[0].error) {
                    // the topic exists in kafka, just never had its offset set (i.e. not been read from)
                    debug(`Error fetching offset for existing topic ${result[0].error}`);
                }

                debug(`[${consumerGroup + '/' + topic}] Got offset data: ${result[0].offset}`);
                this.offsets[consumerGroup][topic] = result[0].offset;
                // Subscribe to the topic
                return this.consumers[consumerGroup][topic].subscribe(topic, partition, { offset: (result[0].offset || 0) + 1 }, (messageSet, topicCombo, partition) => {
                    this.clients[consumerGroup][topic].messageHandler(messageSet, consumerGroup + '/' + topic, partition)
                });

            }).catch((err) => {
                debug(`.catch(error) fetching offset for existent topic: ${err}`);
                // if we end up here, it means the topic does not exist in kafka. reject the web socket
                return this._disconnectClient(consumerGroup, topic, partition, () => {
                    // delete the consumer so we can try again next time
                    delete this.consumers[consumerGroup][topic];
                    return;
                });
            }).error((err) => {
                debug(`.error(error) fetching offset for existent topic: ${err}`);
                return this._disconnectClient(consumerGroup, topic, partition, () => {
                    // delete the consumer so we can try again next time
                    delete this.consumers[consumerGroup][topic];
                    return;
                });
            });
        }
        else {
            // the offset was supplied by the client
            return this.consumers[consumerGroup][topic].subscribe(topic, partition, { offset: requestOffset }, (messageSet, topicCombo, partition) => {
                this.clients[consumerGroup][topic].messageHandler(messageSet, consumerGroup + '/' + topic, partition)
            });
        }
    }

    _getOffset(consumerGroup, topic, partition) {
        // get the current cursor position for this topic 
        return this.consumers[consumerGroup][topic].fetchOffset([
            {
                topic: topic,
                partition: partition
            }
        ]);
    }


    _batchMessageHandler(messageSet, topicCombo, partition) {
        // workaround... nokafka onyl allows three params to be sent to message handlers, so we just concat and then unpack
        let consumerGroup = topicCombo.split('/')[0];
        let topic = topicCombo.split('/')[1];
        let batchSize = messageSet.length - 1;
        this.clients[consumerGroup][topic].received = this.clients[consumerGroup][topic].received + messageSet.length;
        this.clients[consumerGroup][topic].backlog = this.clients[consumerGroup][topic].received - this.clients[consumerGroup][topic].sent; 
        //debug(`[${consumerGroup} / ${topic}] received offset: ${messageSet[batchSize].offset}`);

        if (this.clients[consumerGroup][topic]) {
            this.clients[consumerGroup][topic].ws.send(JSON.stringify(
                messageSet.map((message) => {
                    return {
                        message: message.message.value.toString(), 
                        offset: message.offset
                    }
                })
            ), (err) => this._handleSendResponse(err, this.clients[consumerGroup][topic], consumerGroup, topic, partition, messageSet[batchSize].offset, messageSet.length));
        }
    }

    _outOfRangeHandler(err) {
        debug(`Got offset error on kafka: ${JSON.stringify(err) }`);
    }

    _errorHandler(err) {
        debug(`got error on kafka: ${err}`);
    }

    _handleSendResponse(err, client, consumerGroup, topic, partition, offset, batchSize) {
        if (err) {
            // this case occurs when we have messages from kafka, and trying to send them to a closed web socket
            // no need to do anything in this case other than just silently drop the messages
            debug(`could not send to: ${consumerGroup} / ${topic} as web socket has disconnected`);
        }
        else {
            if (offset % 10000 == 0) {                
                //debug(`[${consumerGroup} / ${topic}] sent offset: ${offset}`);
            }
            // we must check the client is still available before setting the offset
            // in the case when we're swapping the connection over to a conflicting web socket, it won't be
            if (this.clients[consumerGroup][topic]) {
                this.clients[consumerGroup][topic].sent = this.clients[consumerGroup][topic].sent + batchSize;
                this.clients[consumerGroup][topic].backlog = this.clients[consumerGroup][topic].received - this.clients[consumerGroup][topic].sent; 

                // commits will happen after every batch is handled. this will be frequent when messages trickle in and infrequent when we're behind  
                this.consumers[consumerGroup][topic].commitOffset([{ topic: topic, partition: partition, offset: offset }])
                .catch((ex) => {
                    debug(`error setting offset for consumer group [${consumerGroup}], topic [${topic}]: ${ex}`);                   
                });

                this.offsets[consumerGroup][topic] = offset;
            }
        }
    }

    // this case is triggered when we try to send a client which has an error, or a new client connects
    _disconnectClient(consumerGroup, topic, partition, callback) {
        let client = this.clients[consumerGroup][topic];
        
        // this will only be true if we've called disconnect already
        if (!client) return;

        // Remove the client from tracking 
        delete this.clients[consumerGroup][topic];
        debug('removed client from active connection pool');        
        
        // unsubscribe from kafka
        return this.consumers[consumerGroup][topic].unsubscribe([topic], partition).then(() => {
            debug(`Successfully unsubscribed!!! from ${topic}`);

            if (this.offsets[consumerGroup][topic]) {
                // Save the last knwown offset
                debug(`Latest offset is ${this.offsets[consumerGroup][topic]}, writing to kafka`)
                this.consumers[consumerGroup][topic].commitOffset([{ topic: topic, partition: partition, offset: this.offsets[consumerGroup][topic] }]);
            }

            // if the client is not already closing, forcefully close it... this occurs when in the case of conflicting clients
            if (client.ws.readyState != WebSocket.CLOSING && client.ws.readyState != WebSocket.CLOSED) {
                debug(`force closing websocket ${consumerGroup + '/' + topic}`);

                //  ws.close() does work but takes some time to finish sending, so need to listen for a successful close 
                client.ws.close();

                // only need to raise the callback via an event handler, if one has been requested
                client.ws.on('close', () => {
                    // once the client has finished sending, it will close and trigger here
                    // optional callback so we can notify when we're done
                    debug(`force close of websocket ${consumerGroup + '/' + topic} completed`);
                    return callback();
                });
            }
            else {
                return callback();
            }
        });
    }

    _clientFromWebsocket(ws, consumerGroup, topic) {
        return {
            // Client web socket 
            ws: ws,
            // method for handling messages 
            messageHandler: (messageSet, topic, partition) => this._batchMessageHandler(messageSet, topic, partition),
            // Desired offset for the client 
            offset: (this.offsets[consumerGroup][topic]) || -1,
            // Tracking
            partition: 0,
            // Tracking
            received: 0,
            sent: 0,
            backlog: 0,
            createdAt: new Date()
        };
    }

    _verifyClient(info, callback) {
        // optional authorization
        // currently support only basic http 
        if (this.auth && 
        (('basic ' + this.auth) != info.req.headers.authorization && ('Basic ' + this.auth) != info.req.headers.authorization)) {
            debug(`Unauthorized client connection for ${info.req.url}. Rejecting 401`)
            return callback(false, 401, "unauthorized");
        }
        return callback(true);
    }    

    _recordMetrics() {
        debug(`************ REPORTING **************`);
        Object.keys(this.clients).forEach((consumerGroup) => {
            debug(`******* Consumer Group: (${consumerGroup}) *********`);
            Object.keys(this.clients[consumerGroup]).forEach((topicName) => {
                debug(`******* Topic / Partition: ${topicName} / ${this.clients[consumerGroup][topicName].partition} *********`);
                debug(`[${topicName}] latest offset: ${this.offsets[consumerGroup][topicName]}`);
                debug(`[${topicName}] backlog: ${this.clients[consumerGroup][topicName].backlog}`);
                debug(`[${topicName}] received: ${this.clients[consumerGroup][topicName].received}`);
                debug(`[${topicName}] sent: ${this.clients[consumerGroup][topicName].sent}`);
                let duration = (new Date().getTime() - this.clients[consumerGroup][topicName].createdAt.getTime()) / 1000;
                debug(`[${topicName}] duration: ${duration}`);
                debug(`[${topicName}] throughput: ${this.clients[consumerGroup][topicName].sent / duration}`);
            });
        });
        debug(`************************************`);
    }
}

module.exports = KafkaProxy;

