// Global vars
participation_key = "92bfe25f4a04f2e0338ded23ae30af1e482a0709";
resource_id = "f0953e1fecc84dc7a2fe5bf67dd87ed06c997f42";
swarm_id = "3eae4f23d133a9d61c1582ca66cd45191fefcc57";

history_response = false;

// Main
SWARM.connect({apikey: participation_key,
               resource: resource_id,
               swarms: [swarm_id],              
               // callbacks
               onconnect:
                   function onConnect() {
                       console.log("Connected to swarm " + swarm_id);
                       getStoredMessages();
                   },
               onpresence:
                   function onPresence(presence) {
                       var presenceObj = JSON.parse(presence);
                       
                       // exit if not swarm presence
                       if (!isSwarmPresence(presenceObj)) {
                           return;
                       }

                       var resource = presenceObj.presence.from.resource;
                       var type = presenceObj.presence.type;
                           
                       // presence unavailable received
                       if (type && (type == "unavailable")) {
                           console.log("Presence unavailable received from " + resource);
                       // presence available received
                       } else {
                           console.log("Presence available received from " + resource);
                       }                                              
                   },
               onmessage:
                   function onMessage(message) {                       
                       var messageObj = JSON.parse(message);                      
                     
                       // exit if not swarm message
                       if (!isSwarmMessage(messageObj)) {
                           return;
                       }
                       
                       var resource = messageObj.message.from.resource;                       
                       var payload = messageObj.message.payload;

                       // select-response message directed towards this resource received
                       if (isMySelectResponse(payload, resource_id)) {
                           if (isEnd(payload.select_response)) {
                               history_response = false;                  
                               console.log("Finished reading in history response");
                           }
                           
                           if (history_response == true) {
                               var timestamp = payload.select_response.timestamp;
                               var producer = payload.select_response.producer;
                               var message = payload.select_response.message;
                               var currObj = {"timestamp":timestamp, "producer":producer, "message":message};
                               appendMessage(currObj);
                           }
                           
                           if (isBegin(payload.select_response)) {
                               history_response = true; 
                               console.log("Reading in history response");
                           } 
                       }                       
                   },
               onerror:
                   function onError(error) {
                       console.log(error);
                   }
              });

// UI functions
getStoredMessages = function() {
    console.log("Querying the database for the history of messages");
    var toSend = {"history_select": "out"};
    SWARM.send(toSend);
};

appendMessage = function(currObj) {
    var $toAppend = '<li>' + currObj.timestamp + ' | ' + currObj.producer + ' | ' + currObj.message + '</li>';
    $("#messages").append($toAppend);
};

// Conditionals
isSwarmPresence = function(presenceObj) {
    if (presenceObj.presence.from.swarm) {
        return true;
    } else {
        return false;
    }    
};

isSwarmMessage = function(messageObj) {
    if (messageObj.message.from.swarm) {
        return true;
    } else {
        return false;
    }
};

isMySelectResponse = function(payload, resource_id) {
    if (payload.select_response && (payload.to == resource_id)) {
        return true;
    } else {
        return false;
    }
};

isMachineByLocation = function(request_id) {
    if (request_id == "machine_by_location") {
        return true;
    } else {
        return false;
    }
}

isBegin = function(select_response) {
    if (select_response == "begin") {
        return true;
    } else {
        return false;
    }
};

isEnd = function(select_response) {
    if (select_response == "end") {
        return true;
    } else {
        return false;
    }
};