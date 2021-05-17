module.exports = function(RED) {
    const { Kafka } = require('kafkajs');

    const acksDict = {
        "all" : -1,
        "none" : 0,
        "leader": 1
    }

    const delegatedRetriesDict = {
        "SERVER_DOWN": "SERVER_DOWN",
        "SERVER_UP": "SERVER_UP"
    }

    function KafkajsProducerNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
        node.ready = false;

        let client = RED.nodes.getNode(config.client);

        let kafka = new Kafka(client.options);

        if(!client){
            return;
        }

        var producerOptions = new Object();
        var sendOptions = new Object();
        sendOptions.topic = config.topic || null;

        producerOptions.metadataMaxAge = config.metadatamaxage;
        producerOptions.allowAutoTopicCreation = config.allowautotopiccreation;
        producerOptions.transactionTimeout = config.transactiontimeout;
        
        sendOptions.partition = config.partition || null;
        sendOptions.key = config.key || null;
        sendOptions.headers = config.headeritems || {};
        
        sendOptions.acks = acksDict[config.acknowledge];
        sendOptions.timeout = config.responsetimeout;

        node.sendOptions = sendOptions;
        
        node.init = async function init(){
            const producer = kafka.producer();
            node.producer = producer;

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
            
            producer.on(producer.events.CONNECT, node.onConnect);
            producer.on(producer.events.DISCONNECT, node.onDisconnect);
            producer.on(producer.events.REQUEST_TIMEOUT, node.onRequestTimeout);

            await producer.connect();   
        }

        if(!config.delegatedRetries){
        node.init();
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
            if(node.ready){
                if(msg.payload){

                    var sendOptions = new Object();

                    sendOptions.topic = node.sendOptions.topic || msg.topic || null;
                    sendOptions.acks = node.sendOptions.acks || null;
                    sendOptions.timeout = node.sendOptions.timeout || null;

                    sendOptions.messages = [];
                    var message = new Object();
                    
                    message.key = node.sendOptions.key || msg.key || null;
                    
                    message.headers = node.sendOptions.headers;
                    message.headers = Object.keys(message.headers).length === 0 ? msg.headers : message.headers;
                    
                    message.partition = node.sendOptions.partition || msg.partition || null;
                    
                    message.value = msg.payload;
    
                    sendOptions.messages.push(message);

                    node.producer.send(sendOptions).catch((e)=>{
                        node.error("Kafka Producer Error", e);
                        node.status({fill:"red",shape:"ring",text:"Error"});
                    })
    
                    node.lastMessageTime = new Date().getTime();
                    node.status({fill:"blue",shape:"ring",text:"Sending"});
                }
            }else if(!node.ready && msg.payload!=null){
                node.log("Received message while node.ready="+node.ready);
                node.log(msg.payload);
                throw new Error("Kafkajs Producer received message while node is not ready");
            }    
        });

        node.disconnect=async function(){
            node.log("disconnecting");
            await node.producer.disconnect().then(() => {
                node.status({});
                //clearInterval(node.interval);
                // done();
            }).catch(e => {
                node.onError(e);
            });
        }

        node.on('close', function(done){
            node.producer.disconnect().then(() => {
                node.status({});
                clearInterval(node.interval);
                done();
            }).catch(e => {
                node.onError(e);
            });
        });
    }
    RED.nodes.registerType("kafkajs-producer",KafkajsProducerNode);
}
