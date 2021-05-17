module.exports = function(RED) {
    const { Kafka } = require('kafkajs'); 
    const { v4: uuidv4 } = require('uuid');

    const delegatedRetriesDict = {
        "SERVER_DOWN": "SERVER_DOWN",
        "SERVER_UP": "SERVER_UP"
    }

    function KafkajsConsumerNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
        node.ready = false;            
        let client = RED.nodes.getNode(config.client);    
        
        if(!client){
            return;
        }

        //This block is required to suppress infinite loop connections
        if(config.delegatedRetries){
            client.options.retry = client.options.retry || new Object();
            client.options.retry.restartOnFailure=async (err)=>{
                node.log("In restartOnFailure: "+err);
                node.log(JSON.stringify(err));
            }    
        }
        const kafka = new Kafka(client.options)
        
        let consumerOptions = new Object();
        consumerOptions.groupId = config.groupid ? config.groupid : "kafka_js_" + uuidv4();

        let subscribeOptions = new Object();
        subscribeOptions.topic = config.topic;

        let runOptions = new Object();

        if(config.advancedoptions){
            consumerOptions.sessionTimeout =  config.sessiontimeout;
            consumerOptions.rebalanceTimeout =  config.rebalancetimeout;
            consumerOptions.heartbeatInterval =  config.heartbeatinterval;
            consumerOptions.metadataMaxAge =  config.metadatamaxage;
            consumerOptions.allowAutoTopicCreation = config.allowautotopiccreation;
            consumerOptions.maxBytesPerPartition =  config.maxbytesperpartition;
            consumerOptions.minBytes =  config.minbytes;
            consumerOptions.maxBytes =  config.maxbytes;
            consumerOptions.maxWaitTimeInMs =  config.maxwaittimeinms;

            subscribeOptions.fromBeginning = config.frombeginning;

            runOptions.autoCommitInterval =  config.autocommitinterval;
            runOptions.autoCommitThreshold =  config.autocommitthreshold; 
        }



        node.init = async function init(){
            if(config.advancedoptions && config.clearoffsets){
                node.status({fill:"yellow",shape:"ring",text:"Clearing Offset"});
                var admin = kafka.admin();
                await admin.connect();
                await admin.resetOffsets({groupId:config.groupid, topic:config.topic});
                await admin.disconnect()
            }

            node.consumer = kafka.consumer(consumerOptions);
            node.status({fill:"yellow",shape:"ring",text:"Initializing"});

            node.onConnect = function (){
                node.ready = true;
                node.lastMessageTime = new Date().getTime();
                node.status({fill:"green",shape:"ring",text:"Ready"});
            }
    
            node.onDisconnect = function (){
                node.ready = false;
                node.status({fill:"red",shape:"ring",text:"Offline"});
            }
    
            node.onRequestTimeout = function (){
                node.status({fill:"red",shape:"ring",text:"Timeout"});
            }

            node.onError = function (e){
                node.error("Kafka Consumer Error", e.message);
                node.status({fill:"red",shape:"ring",text:"Error"});
            }
    
            node.onMessage = function(topic, partition, message){
                node.lastMessageTime = new Date().getTime();
                var payload = new Object();
                payload.topic = topic;
                payload.partition = partition;
                
                payload.payload = new Object();
                payload.payload = message;
                
                payload.payload.key= message.key ? message.key.toString() : null;
                payload.payload.value = message.value.toString();

                for(const [key, value] of Object.entries(payload.payload.headers)){
                    payload.payload.headers[key] = value.toString();
                }
                
                node.send(payload);	
                node.status({fill:"blue",shape:"ring",text:"Reading"});
            }
    
            function checkLastMessageTime() {
                if(node.ready){//we only want to reset to Idle when node is working fine.
                if(node.lastMessageTime != null){
                    timeDiff = new Date().getTime() - node.lastMessageTime;
                    if(timeDiff > 5000){
                        node.status({fill:"yellow",shape:"ring",text:"Idle"});
                    } 
                }   
            }
            }
              
            node.interval = setInterval(checkLastMessageTime, 1000);
    
            node.consumer.on(node.consumer.events.CONNECT, node.onConnect);
            node.consumer.on(node.consumer.events.DISCONNECT, node.onDisconnect);
            node.consumer.on(node.consumer.events.REQUEST_TIMEOUT, node.onRequestTimeout);

            await node.consumer.connect();
            await node.consumer.subscribe(subscribeOptions);

            runOptions.eachMessage = async ({ topic, partition, message }) => {
                node.onMessage(topic, partition, message);
            }

            await node.consumer.run(runOptions);
        }
        node.on('input', function(msg) {
            if(config.delegatedRetries){
                if(msg.serviceStatus && msg.payload==null){
                    //this is a service status message. 
                    node.log("Input>>Service Status message received: "+msg.serviceStatus);
                    if(msg.serviceStatus==delegatedRetriesDict.SERVER_DOWN){
                        //close open connection
                        node.disconnect()
                        .then(node.onDisconnect);
                    }else if(msg.serviceStatus==delegatedRetriesDict.SERVER_UP && !node.ready){
                        //open new connection
                        node.init()
                        .then(node.onConnect);
                    }else{
                        node.log("Not doing anything with service status")
                    }
                }else if(msg.serviceStatus && msg.payload!=null){
                    throw new Error("Bad Input: Service Status cannot carry payload")
                }
            }
        });

        if(!config.delegatedRetries){
            node.init().catch( e => {
                node.onError(e);
            });
        }

        node.disconnect=async function(){
            node.log("disconnecting");
            await node.consumer.disconnect().then(() => {
                node.status({});
                //clearInterval(node.interval);
                // done();
            }).catch(e => {
                node.onError(e);
            });
        }

        node.on('close', function(done){
            node.consumer.disconnect().then(() => {
                node.status({});
                clearInterval(node.interval);
                done();
            }).catch(e => {
                node.onError(e);
            });
		});
    }
    RED.nodes.registerType("kafkajs-consumer",KafkajsConsumerNode);
}
