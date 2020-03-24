/*
 * (C) Copyright 2014 Kurento (http://kurento.org/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * (C) Copyright 2017 Novakid (http://novakidschool.com/)
 *
 *
 */

var path = require('path');
var express = require('express');
var ws = require('ws');
var socketio = require('socket.io');
var minimist = require('minimist');
var url = require('url');
var kurento = require('kurento-client');
var fs = require('fs');
var https = require('https');
var http = require('http');
var Buffer = require('buffer').Buffer;

var argv = minimist(process.argv.slice(2), {
    default: {
        hostname: 'localhost',
        port: 8889,
        ws_uri: 'ws://localhost:8888/kurento',
        records_path: 'file:///tmp/novakid_records/',
        stats_timeout: 10,
        syslog: 1,
        ssl: 1
    }
});

var options = {
    hostname: 'localhost',
    key:  fs.readFileSync('keys/server.key'),
    cert: fs.readFileSync('keys/server.crt')
};

if (argv.syslog) {
    var logger = new (require('ain2'))({
        tag: 'novakid-kurento-client',
        facility: 'local1',
        path: '/dev/log',
        messageComposer: function (message, severity) {
            return new Buffer('<' + (this.facility * 8 + severity) + '>' + this.tag + ': ' + message);
        }
    });
}

if (argv.hostname) {
    options.hostname = argv.hostname;
}

//argv.ssl = 1;

/*
 * Definition of global variables.
 */

var kurentoClient = null;
var userRegistry = new UserRegistry();
var pipelines = {};
var candidatesQueue = {};
var idCounter = 0;

function nextUniqueId () {
    idCounter++;
    return idCounter.toString();
}

function setBandwidth (ep, min, max) {
    ep.setMinVideoSendBandwidth(min);
    ep.setMinVideoRecvBandwidth(min);
    ep.setMaxVideoSendBandwidth(max);
    ep.setMaxVideoRecvBandwidth(max);
}

function sendMessage (ws, message) {
    try {
        if (ws.$$$socketio) {
            return ws.emit('message', message);
        }
        return ws.send(JSON.stringify(message));
    } catch (err) {}
}

function sendLogMessage (user, message) {
    if (typeof user === 'string') {
        user = userRegistry.getById(user);
    }
    message = Object.assign({
        timestamp: new Date().getTime()
    }, user && user.opts ? user.opts.user || {} : {}, message);
    if (message.json_data && !message.data) {
        try {
            message.data = JSON.stringify(message.json_data).replace(/:/g, ': ').replace(/,/g, ', ');
        } catch (exception) {
            console.log(exception);
        }
    }
    if (argv.syslog) {
        try {
            logger.log(JSON.stringify(message));
        } catch (exception) {
            console.log(exception);
        }
    } else {
        console.log('\n#', message, '\n');
    }
}

// Recover kurentoClient for the first time.
function getKurentoClient (callback) {
    if (kurentoClient !== null) {
        return callback(null, kurentoClient);
    }

    kurento(argv.ws_uri, function (error, _kurentoClient) {
        if (error) {
            var message = 'Coult not find media server at address ' + argv.ws_uri;
            return callback(message + '. Exiting with error ' + error);
        }

        kurentoClient = _kurentoClient;
        callback(null, kurentoClient);
    });
}

/*
 * Definition of helper classes
 */

// Represents caller and callee sessions
function UserSession (id, name, callee, opts, ws) {
    this.id = id;
    this.name = name;
    this.callee = callee;
    this.opts = opts || {};
    this.ws = ws;
    this.peer = null;
    this.sdpOffer = null;
}

UserSession.prototype.sendMessage = function (message) {
    sendMessage(this.ws, message);
}

// Represents registrar of users
function UserRegistry() {
    this.usersById = {};
    this.usersByName = {};
}

UserRegistry.prototype.register = function (user) {
    this.usersById[user.id] = user;
    this.usersByName[user.name] = user;
}

UserRegistry.prototype.unregister = function (id) {
    var user = this.getById(id);
    if (user) delete this.usersById[id];
    if (user && this.getByName(user.name)) delete this.usersByName[user.name];
}

UserRegistry.prototype.getById = function (id) {
    return this.usersById[id];
}

UserRegistry.prototype.getByName = function (name) {
    return this.usersByName[name];
}

UserRegistry.prototype.removeById = function (id) {
    var userSession = this.usersById[id];
    if (!userSession) return;
    delete this.usersById[id];
    delete this.usersByName[userSession.name];
}

UserRegistry.prototype.getByCallee = function (callee) {
    for (var key in this.usersById) {
        if (this.usersById.hasOwnProperty(key)) {
            if (this.usersById[key].callee === callee) {
                return this.usersById[key];
            }
        }
    }
}

function CallMediaPipelineMonitor (callerId, callerEp, calleeId, calleeEp) {
    this.callerId = callerId;
    this.callerEp = callerEp;
    this.calleeId = calleeId;
    this.calleeEp = calleeEp;

    this.onCallerEpConnectionStateChanged = this.onCallerEpConnectionStateChanged.bind(this);
    this.onCalleeEpConnectionStateChanged = this.onCalleeEpConnectionStateChanged.bind(this);
    this.onCallerEpMediaStateChanged = this.onCallerEpMediaStateChanged.bind(this);
    this.onCalleeEpMediaStateChanged = this.onCalleeEpMediaStateChanged.bind(this);
    this.onCallerEpMediaFlowInStateChange = this.onCallerEpMediaFlowInStateChange.bind(this);
    this.onCalleeEpMediaFlowInStateChange = this.onCalleeEpMediaFlowInStateChange.bind(this);
    this.onCallerEpMediaFlowOutStateChange = this.onCallerEpMediaFlowOutStateChange.bind(this);
    this.onCalleeEpMediaFlowOutStateChange = this.onCalleeEpMediaFlowOutStateChange.bind(this);

    this.callerEp.on('ConnectionStateChanged', this.onCallerEpConnectionStateChanged);
    this.calleeEp.on('ConnectionStateChanged', this.onCalleeEpConnectionStateChanged);

    this.callerEp.on('MediaStateChanged', this.onCallerEpMediaStateChanged);
    this.calleeEp.on('MediaStateChanged', this.onCalleeEpMediaStateChanged);

    this.callerEp.on('MediaFlowInStateChange', this.onCallerEpMediaFlowInStateChange);
    this.calleeEp.on('MediaFlowInStateChange', this.onCalleeEpMediaFlowInStateChange);

    this.callerEp.on('MediaFlowOutStateChange', this.onCallerEpMediaFlowOutStateChange);
    this.calleeEp.on('MediaFlowOutStateChange', this.onCalleeEpMediaFlowOutStateChange);
}

CallMediaPipelineMonitor.prototype.stop = function () {
    //
}

CallMediaPipelineMonitor.prototype.sendMessage = function (owner, data) {
    var caller = userRegistry.getById(this.callerId),
        callee = userRegistry.getById(this.calleeId);
    if (caller) {
        caller.sendMessage(Object.assign(data, {my: this.callerId === owner}));
    }
    if (callee) {
        callee.sendMessage(Object.assign(data, {my: this.callerId === owner}));
    }
};

CallMediaPipelineMonitor.prototype.onCallerEpConnectionStateChanged = function (evt) {
    this.sendMessage(this.callerId, {id: 'stateConnectionStateChanged', state: evt.newState});
};

CallMediaPipelineMonitor.prototype.onCalleeEpConnectionStateChanged = function (evt) {
    this.sendMessage(this.calleeId, {id: 'stateConnectionStateChanged', state: evt.newState});
};

CallMediaPipelineMonitor.prototype.onCallerEpMediaStateChanged = function (evt) {
    this.sendMessage(this.callerId, {id: 'stateMediaStateChanged', state: evt.newState});
};

CallMediaPipelineMonitor.prototype.onCalleeEpMediaStateChanged = function (evt) {
    this.sendMessage(this.calleeId, {id: 'stateMediaStateChanged', state: evt.newState});
};

CallMediaPipelineMonitor.prototype.onCallerEpMediaFlowInStateChange = function (evt) {
    this.sendMessage(this.callerId, {id: 'stateMediaFlowInStateChange', state: evt.state, type: evt.mediaType});
};

CallMediaPipelineMonitor.prototype.onCalleeEpMediaFlowInStateChange = function (evt) {
    this.sendMessage(this.calleeId, {id: 'stateMediaFlowInStateChange', state: evt.state, type: evt.mediaType});
};

CallMediaPipelineMonitor.prototype.onCallerEpMediaFlowOutStateChange = function (evt) {
    this.sendMessage(this.callerId, {id: 'stateMediaFlowOutStateChange', state: evt.state, type: evt.mediaType});
};

CallMediaPipelineMonitor.prototype.onCalleeEpMediaFlowOutStateChange = function (evt) {
    this.sendMessage(this.calleeId, {id: 'stateMediaFlowOutStateChange', state: evt.state, type: evt.mediaType});
};

function CallMediaPipelineStator (callerId, callerEp, calleeId, calleeEp) {
    this.callerId = callerId;
    this.callerEp = callerEp;
    this.callerConnected = false;
    this.calleeId = calleeId;
    this.calleeEp = calleeEp;
    this.calleeConnected = false;
    this.lastStats = {};
    this.timeout = undefined;

    this.check = this.check.bind(this);
    this.onCallerEpConnected = this.onCallerEpConnected.bind(this);
    this.onCalleeEpConnected = this.onCalleeEpConnected.bind(this);
    this.onGetCallerEpStats = this.onGetCallerEpStats.bind(this);
    this.onGetCalleeEpStats = this.onGetCalleeEpStats.bind(this);
    this.onCallerEpIceComponentStateChange = this.onCallerEpIceComponentStateChange.bind(this);
    this.onCalleeEpIceComponentStateChange = this.onCalleeEpIceComponentStateChange.bind(this);
    this.onCallerEpNewCandidatePairSelected = this.onCallerEpNewCandidatePairSelected.bind(this);
    this.onCalleeEpNewCandidatePairSelected = this.onCalleeEpNewCandidatePairSelected.bind(this);
    this.onCallerEpIceCandidateFound = this.onCallerEpIceCandidateFound.bind(this);
    this.onCalleeEpIceCandidateFound = this.onCalleeEpIceCandidateFound.bind(this);
    this.onCallerEpIceGatheringDone = this.onCallerEpIceGatheringDone.bind(this);
    this.onCalleeEpIceGatheringDone = this.onCalleeEpIceGatheringDone.bind(this);
    this.onCallerEpConnectionStateChanged = this.onCallerEpConnectionStateChanged.bind(this);
    this.onCalleeEpConnectionStateChanged = this.onCalleeEpConnectionStateChanged.bind(this);
    this.onCallerEpMediaStateChanged = this.onCallerEpMediaStateChanged.bind(this);
    this.onCalleeEpMediaStateChanged = this.onCalleeEpMediaStateChanged.bind(this);
    this.onCallerEpMediaFlowInStateChange = this.onCallerEpMediaFlowInStateChange.bind(this);
    this.onCalleeEpMediaFlowInStateChange = this.onCalleeEpMediaFlowInStateChange.bind(this);
    this.onCallerEpMediaFlowOutStateChange = this.onCallerEpMediaFlowOutStateChange.bind(this);
    this.onCalleeEpMediaFlowOutStateChange = this.onCalleeEpMediaFlowOutStateChange.bind(this);

    this.callerEp.on('MediaStateChanged', this.onCallerEpConnected);
    this.calleeEp.on('MediaStateChanged', this.onCalleeEpConnected);

    this.callerEp.on('IceComponentStateChange', this.onCallerEpIceComponentStateChange);
    this.calleeEp.on('IceComponentStateChange', this.onCalleeEpIceComponentStateChange);

    this.callerEp.on('NewCandidatePairSelected', this.onCallerEpNewCandidatePairSelected);
    this.calleeEp.on('NewCandidatePairSelected', this.onCalleeEpNewCandidatePairSelected);

    this.callerEp.on('IceCandidateFound', this.onCallerEpIceCandidateFound);
    this.calleeEp.on('IceCandidateFound', this.onCalleeEpIceCandidateFound);

    this.callerEp.on('IceGatheringDone', this.onCallerEpIceGatheringDone);
    this.calleeEp.on('IceGatheringDone', this.onCalleeEpIceGatheringDone);

    this.callerEp.on('ConnectionStateChanged', this.onCallerEpConnectionStateChanged);
    this.calleeEp.on('ConnectionStateChanged', this.onCalleeEpConnectionStateChanged);

    this.callerEp.on('MediaStateChanged', this.onCallerEpMediaStateChanged);
    this.calleeEp.on('MediaStateChanged', this.onCalleeEpMediaStateChanged);

    this.callerEp.on('MediaFlowInStateChange', this.onCallerEpMediaFlowInStateChange);
    this.calleeEp.on('MediaFlowInStateChange', this.onCalleeEpMediaFlowInStateChange);

    this.callerEp.on('MediaFlowOutStateChange', this.onCallerEpMediaFlowOutStateChange);
    this.calleeEp.on('MediaFlowOutStateChange', this.onCalleeEpMediaFlowOutStateChange);
}

CallMediaPipelineStator.prototype.onConnected = function () {
    this.start();
};

CallMediaPipelineStator.prototype.onCallerEpConnected = function (evt) {
    if (evt.newState === 'CONNECTED') {
        this.callerConnected = true;
    }
    if (this.callerConnected && this.calleeConnected) {
        this.onConnected();
    }
};

CallMediaPipelineStator.prototype.onCalleeEpConnected = function (evt) {
    if (evt.newState === 'CONNECTED') {
        this.calleeConnected = true;
    }
    if (this.callerConnected && this.calleeConnected) {
        this.onConnected();
    }
};

CallMediaPipelineStator.prototype.getStat = function (stat) {
    return {
        id: stat.id,
        type: 'video_' + stat.type,
        bytes: stat.bytesReceived || stat.bytesSent || 0,
        bytesTransferred: 0,
        packetsLost: stat.packetsLost,
        packets: stat.packetsReceived || stat.packetsSent || 0
        //firCount: stat.firCount,
        //fractionLost: stat.fractionLost,
        //nackCount: stat.nackCount,
        //pliCount: stat.pliCount,
        //remb: stat.remb,
        //sliCount: stat.sliCount,
        //ssrc: stat.ssrc,
        //timestamp: stat.timestamp
    };
};

CallMediaPipelineStator.prototype.start = function () {
    clearTimeout(this.timeout);
    this.timeout = undefined;
    this.check();
}

CallMediaPipelineStator.prototype.stop = function () {
    clearTimeout(this.timeout);
    this.timeout = null;
    this.callerEp = null;
    this.calleeEp = null;
}

CallMediaPipelineStator.prototype.check = function () {
    if (!this.timeout === null || !this.callerEp || !this.calleeEp) {
        return undefined;
    }
    var mediaType = 'VIDEO';
    this.callerEp.getStats(mediaType, this.onGetCallerEpStats);
    this.calleeEp.getStats(mediaType, this.onGetCalleeEpStats);
    this.timeout = setTimeout(this.check, argv.stats_timeout * 1000);
};

CallMediaPipelineStator.prototype.onGetStats = function (error, statsMap, userId, secondUserId) {
    var user = userRegistry.getById(userId),
        secondUser = userRegistry.getById(secondUserId);
    if (!user || !secondUser) {
        return undefined;
    }
    if (error) {
        sendLogMessage(user, {json_data: {
            msg: 'stats',
            data: null
        }});
        return undefined;
    }
    var stats = [];
    for (var key in statsMap) {
        if (statsMap.hasOwnProperty(key)) {
            var stat = this.getStat(statsMap[key]),
                lastStat = this.lastStats[key] || {bytes: 0};
            stat.bytesTransferred = stat.bytes - lastStat.bytes;
            stats.push(stat);
            this.lastStats[key] = {bytes: stat.bytes};
        }
    }
    user.sendMessage({id: 'stats', my: true, stats: stats || null});
    secondUser.sendMessage({id: 'stats', my: false, stats: stats || null});
    sendLogMessage(user, {json_data: {
        msg: 'stats',
        data: stats || null
    }});
};

CallMediaPipelineStator.prototype.onGetCallerEpStats = function (error, statsMap) {
    if (this.timeout === null) {
        return undefined;
    }
    this.onGetStats(error, statsMap, this.callerId, this.calleeId);
};

CallMediaPipelineStator.prototype.onGetCalleeEpStats = function (error, statsMap) {
    if (this.timeout === null) {
        return undefined;
    }
    this.onGetStats(error, statsMap, this.calleeId, this.callerId);
};

CallMediaPipelineStator.prototype.sendMessage = function (userId, data) {
    var user = userRegistry.getById(userId);
    if (!user) {
        return undefined;
    }
    sendLogMessage(user, {json_data: data});
};

CallMediaPipelineStator.prototype.onIceComponentStateChange = function (evt, userId) {
    this.sendMessage(userId, {json_data: {
        msg: 'IceComponentStateChange',
        data: {
            source: evt.source,
            state: evt.state
        }
    }});
};

CallMediaPipelineStator.prototype.onCallerEpIceComponentStateChange = function (evt) {
    this.onIceComponentStateChange(evt, this.callerId);
};

CallMediaPipelineStator.prototype.onCalleeEpIceComponentStateChange = function (evt) {
    this.onIceComponentStateChange(evt, this.calleeId);
};

CallMediaPipelineStator.prototype.onNewCandidatePairSelected = function (evt, userId) {
    this.sendMessage(userId, {json_data: {
        msg: 'NewCandidatePairSelected',
        data: {
            local: evt.candidatePair.localCandidate,
            remote: evt.candidatePair.remoteCandidate
        }
    }});
};

CallMediaPipelineStator.prototype.onCallerEpNewCandidatePairSelected = function (evt) {
    this.onNewCandidatePairSelected(evt, this.callerId);
};

CallMediaPipelineStator.prototype.onCalleeEpNewCandidatePairSelected = function (evt) {
    this.onNewCandidatePairSelected(evt, this.calleeId);
};

CallMediaPipelineStator.prototype.onIceCandidateFound = function (evt, userId) {
    this.sendMessage(userId, {json_data: {
        msg: 'IceCandidateFound',
        data: {
            source: evt.source,
            candidate: evt.candidate.candidate
        }
    }});
};

CallMediaPipelineStator.prototype.onCallerEpIceCandidateFound = function (evt) {
    this.onIceCandidateFound(evt, this.callerId);
};

CallMediaPipelineStator.prototype.onCalleeEpIceCandidateFound = function (evt) {
    this.onIceCandidateFound(evt, this.calleeId);
};

CallMediaPipelineStator.prototype.onIceGatheringDone = function (evt, userId) {
    this.sendMessage(userId, {json_data: {
        msg: 'IceGatheringDone',
        data: {
            source: evt.source
        }
    }});
};

CallMediaPipelineStator.prototype.onCallerEpIceGatheringDone = function (evt) {
    this.onIceGatheringDone(evt, this.callerId);
};

CallMediaPipelineStator.prototype.onCalleeEpIceGatheringDone = function (evt) {
    this.onIceGatheringDone(evt, this.calleeId);
};

CallMediaPipelineStator.prototype.onConnectionStateChanged = function (evt, userId) {
    this.sendMessage(userId, {json_data: {
        msg: 'ConnectionStateChanged',
        data: {
            source: evt.source,
            newState: evt.newState,
            oldState: evt.oldState
        }
    }});
};

CallMediaPipelineStator.prototype.onCallerEpConnectionStateChanged = function (evt) {
    this.onConnectionStateChanged(evt, this.callerId);
};

CallMediaPipelineStator.prototype.onCalleeEpConnectionStateChanged = function (evt) {
    this.onConnectionStateChanged(evt, this.calleeId);
};

CallMediaPipelineStator.prototype.onMediaStateChanged = function (evt, userId) {
    this.sendMessage(userId, {json_data: {
        msg: 'MediaStateChanged',
        data: {
            source: evt.source,
            newState: evt.newState,
            oldState: evt.oldState
        }
    }});
};

CallMediaPipelineStator.prototype.onCallerEpMediaStateChanged = function (evt) {
    this.onMediaStateChanged(evt, this.callerId);
};

CallMediaPipelineStator.prototype.onCalleeEpMediaStateChanged = function (evt) {
    this.onMediaStateChanged(evt, this.calleeId);
};

CallMediaPipelineStator.prototype.onMediaFlowInStateChange = function (evt, userId) {
    this.sendMessage(userId, {json_data: {
        msg: 'MediaFlowInStateChange',
        data: {
            source: evt.source,
            mediaType: evt.mediaType,
            state: evt.state
        }
    }});
};

CallMediaPipelineStator.prototype.onCallerEpMediaFlowInStateChange = function (evt) {
    this.onMediaFlowInStateChange(evt, this.callerId);
};

CallMediaPipelineStator.prototype.onCalleeEpMediaFlowInStateChange = function (evt) {
    this.onMediaFlowInStateChange(evt, this.calleeId);
};

CallMediaPipelineStator.prototype.onMediaFlowOutStateChange = function (evt, userId) {
    this.sendMessage(userId, {json_data: {
        msg: 'MediaFlowOutStateChange',
        data: {
            source: evt.source,
            mediaType: evt.mediaType,
            state: evt.state
        }
    }});
};

CallMediaPipelineStator.prototype.onCallerEpMediaFlowOutStateChange = function (evt) {
    this.onMediaFlowOutStateChange(evt, this.callerId);
};

CallMediaPipelineStator.prototype.onCalleeEpMediaFlowOutStateChange = function (evt) {
    this.onMediaFlowOutStateChange(evt, this.calleeId);
};


// Represents a B2B active call
function CallMediaPipeline() {
    this.pipeline = null;
    this.callerRecorderEndpoint = null;
    this.calleeRecorderEndpoint = null;
    this.webRtcEndpoint = {};
    this.stator = null;
    this.monitor = null;

    this.release = this.release.bind(this);
}

CallMediaPipeline.prototype.createPipeline = function (callerId, calleeId, bandwidth, ws, callback) {
    var self = this,
        callerName = (userRegistry.getById(callerId) || {name: callerId}).name,
        calleeName = (userRegistry.getById(calleeId) || {name: calleeId}).name,
        piplineTimestamp = new Date().getTime(),
        callerRecorderUri = argv.records_path + callerName + 't' + piplineTimestamp + '_caller.webm',
        calleeRecorderUri = argv.records_path + calleeName + 't' + piplineTimestamp + '_caller.webm';
    getKurentoClient(function (error, kurentoClient) {
        if (error) {
            return callback(error);
        }

        kurentoClient.create('MediaPipeline', function (error, pipeline) {
            if (error) {
                return callback(error);
            }

            pipeline.create([
                {type: 'RecorderEndpoint', params: {uri: callerRecorderUri}},
                {type: 'WebRtcEndpoint', params: {}}
            ], function (error, elements) {
                if (error) {
                    pipeline.release();
                    return callback(error);
                }

                var callerRecorderEndpoint = elements[0],
                    callerWebRtcEndpoint = elements[1];

                if (bandwidth) {
                    setBandwidth(callerWebRtcEndpoint, 100, bandwidth);
                }

                if (candidatesQueue[callerId]) {
                    while(candidatesQueue[callerId].length) {
                        var candidate = candidatesQueue[callerId].shift();
                        callerWebRtcEndpoint.addIceCandidate(candidate);
                    }
                }

                callerWebRtcEndpoint.on('OnIceCandidate', function (event) {
                    var user = userRegistry.getById(callerId);
                    if (!user) {
                        return undefined;
                    }
                    var candidate = kurento.getComplexType('IceCandidate')(event.candidate);
                    user.sendMessage({id: 'iceCandidate', candidate: candidate});
                });

                pipeline.create([
                    {type: 'RecorderEndpoint', params: {uri: calleeRecorderUri}},
                    {type: 'WebRtcEndpoint', params: {}}
                ], function (error, elements) {
                    if (error) {
                        pipeline.release();
                        return callback(error);
                    }

                    var calleeRecorderEndpoint = elements[0],
                        calleeWebRtcEndpoint = elements[1];

                    if (bandwidth) {
                        setBandwidth(calleeWebRtcEndpoint, 100, bandwidth);
                    }

                    if (candidatesQueue[calleeId]) {
                        while(candidatesQueue[calleeId].length) {
                            var candidate = candidatesQueue[calleeId].shift();
                            calleeWebRtcEndpoint.addIceCandidate(candidate);
                        }
                    }

                    calleeWebRtcEndpoint.on('OnIceCandidate', function (event) {
                        var user = userRegistry.getById(calleeId);
                        if (!user) {
                            return undefined;
                        }
                        var candidate = kurento.getComplexType('IceCandidate')(event.candidate);
                        user.sendMessage({
                            id: 'iceCandidate',
                            candidate: candidate
                        });
                    });

                    callerWebRtcEndpoint.connect(calleeWebRtcEndpoint, function (error) {
                        if (error) {
                            pipeline.release();
                            return callback(error);
                        }

                        calleeWebRtcEndpoint.connect(callerWebRtcEndpoint, function (error) {
                            if (error) {
                                pipeline.release();
                                return callback(error);
                            }
                        });

                        kurentoClient.connect(callerWebRtcEndpoint, callerRecorderEndpoint, function (error) {
                            callerRecorderEndpoint.record(function (error) {
                                console.log('Caller ' + callerId + ' start record');
                            });
                        });

                        kurentoClient.connect(calleeWebRtcEndpoint, calleeRecorderEndpoint, function (error) {
                            calleeRecorderEndpoint.record(function (error) {
                                console.log('Callee ' + calleeId + ' start record');
                            });
                        });

                        self.pipeline = pipeline;
                        self.callerRecorderEndpoint = callerRecorderEndpoint;
                        self.calleeRecorderEndpoint = calleeRecorderEndpoint;
                        self.webRtcEndpoint[callerId] = callerWebRtcEndpoint;
                        self.webRtcEndpoint[calleeId] = calleeWebRtcEndpoint;
                        self.stator = new CallMediaPipelineStator(callerId, callerWebRtcEndpoint, calleeId, calleeWebRtcEndpoint);
                        self.monitor = new CallMediaPipelineMonitor(callerId, callerWebRtcEndpoint, calleeId, calleeWebRtcEndpoint);
                        setTimeout(self.release, 60 * 60 * 1000);
                        callback(null);
                    });
                });
            });
        });
    })
}

CallMediaPipeline.prototype.generateSdpAnswer = function (id, sdpOffer, callback) {
    this.webRtcEndpoint[id].processOffer(sdpOffer, callback);
    this.webRtcEndpoint[id].gatherCandidates(function (error) {
        if (error) {
            return callback(error);
        }
    });
}

CallMediaPipeline.prototype.release = function () {
    if (this.callerRecorderEndpoint) this.callerRecorderEndpoint.stop();
    this.callerRecorderEndpoint = null;
    if (this.calleeRecorderEndpoint) this.calleeRecorderEndpoint.stop();
    this.calleeRecorderEndpoint = null;
    if (this.pipeline) this.pipeline.release();
    this.pipeline = null;
    if (this.stator) this.stator.stop();
    this.stator = null;
    if (this.monitor) this.monitor.stop();
    this.monitor = null;
}

function LoopbackCallMediaPipeline() {
    this.pipeline = null;
    this.recorderEndpoint = null;
    this.webRtcEndpoint = {};

    this.release = this.release.bind(this);
}

LoopbackCallMediaPipeline.prototype.createPipeline = function (id, ws, callback) {
    var self = this,
        user = userRegistry.getById(id),
        name = (user || {name: id}).name || id,
        userRecorderUri = argv.records_path + 'user' + name + 't' + new Date().getTime() + '.webm';
    if (user && user.opts && user.opts.recorder) {
        userRecorderUri = user.opts.recorder.uri || userRecorderUri;
    }
    getKurentoClient(function (error, kurentoClient) {
        if (error) {
            return callback(error);
        }

        kurentoClient.create('MediaPipeline', function (error, pipeline) {
            if (error) {
                return callback(error);
            }

            pipeline.create([
                {type: 'RecorderEndpoint', params: {uri: userRecorderUri}},
                {type: 'WebRtcEndpoint', params: {}}
            ], function (error, elements) {
                if (error) {
                    pipeline.release();
                    return callback(error);
                }

                var recorderEndpoint = elements[0],
                    webRtcEndpoint = elements[1];

                if (candidatesQueue[id]) {
                    while(candidatesQueue[id].length) {
                        var candidate = candidatesQueue[id].shift();
                        webRtcEndpoint.addIceCandidate(candidate);
                    }
                }

                webRtcEndpoint.on('OnIceCandidate', function (event) {
                    var user = userRegistry.getById(id);
                    if (user) {
                        var candidate = kurento.getComplexType('IceCandidate')(event.candidate);
                        user.sendMessage({id: 'iceCandidate', candidate: candidate});
                    }
                });

                webRtcEndpoint.connect(webRtcEndpoint, function (error) {
                    if (error) {
                        pipeline.release();
                        return callback(error);
                    }

                    kurentoClient.connect(webRtcEndpoint, recorderEndpoint, function (error) {
                        recorderEndpoint.record(function (error) {
                            console.log('User ' + name + ' start record');
                        });
                    });

                    self.pipeline = pipeline;
                    self.recorderEndpoint = recorderEndpoint;
                    self.webRtcEndpoint[id] = webRtcEndpoint;
                    setTimeout(self.release, 60 * 60 * 1000);
                    callback(null);
                });
            });
        });
    })
}

LoopbackCallMediaPipeline.prototype.generateSdpAnswer = function (id, sdpOffer, callback) {
    this.webRtcEndpoint[id].processOffer(sdpOffer, callback);
    this.webRtcEndpoint[id].gatherCandidates(function (error) {
        if (error) {
            return callback(error);
        }
    });
}

LoopbackCallMediaPipeline.prototype.release = function () {
    if (this.recorderEndpoint) this.recorderEndpoint.stop();
    this.recorderEndpoint = null;
    if (this.pipeline) this.pipeline.release();
    this.pipeline = null;
}

function status (sessionId, ws) {
    function onError (error) {
        sendMessage(ws, {id: 'statusResponse', error: error});
    }

    function getPipelinesInfo (server, callback) {
        server.getPipelines(function (error, pipelines) {
            if (error) {
                return callback(error);
            }

            if (!pipelines || !pipelines.length) {
                return callback(null, pipelines);
            }

            pipelines.forEach(function (pipeline, ind, items) {
                pipeline.getChildren(function (error, childs) {
                    items[ind].childs = childs;
                    if (ind === items.length - 1) {
                        return callback(null, items);
                    }
                });
            });
        });
    }

    getKurentoClient(function (error, kurentoClient) {
        kurentoClient.getServerManager(function (error, server) {
            if (error) {
                return onError(error);
            }
            server.getInfo(function (error, serverInfo) {
                if (error) {
                  return onError(error);
                }

                getPipelinesInfo(server, function (error, pipelinesInfo) {
                    if (error) {
                        return onError(error);
                    }

                    var pipelinesNumber = Object.keys(pipelinesInfo).length;
                    serverInfo.pipelinesNumber = pipelinesNumber;
                    serverInfo.pipelines = pipelinesInfo;
                    sendMessage(ws, {id: 'statusResponse', info: serverInfo});
                });
            })
        });
    });
}

function stop (sessionId) {
    if (!pipelines[sessionId]) {
        return undefined;
    }

    var pipeline = pipelines[sessionId];
    var pipelineId = pipeline.pipeline ? pipeline.pipeline.id : undefined;
    delete pipelines[sessionId];
    pipeline.release();
    var stopperUser = userRegistry.getById(sessionId);
    var stoppedUser = userRegistry.getByName(stopperUser.peer);
    stopperUser.peer = null;

    if (stopperUser) {
        stopperUser.sendMessage({id: 'resetCommunication', message: 'reset communication'});
    }

    if (stoppedUser) {
        stoppedUser.peer = null;
        delete pipelines[stoppedUser.id];
        stoppedUser.sendMessage({id: 'stopCommunication', message: 'remote user hanged out'});
    }

    clearCandidatesQueue(sessionId);

    if (pipelineId) {
        sendLogMessage(stopperUser, {json_data: {
            msg: 'pipeline released',
            data: {pipeline: pipelineId}
        }});
    }
}

function incomingCallResponse (calleeId, from, callResponse, calleeSdp, ws) {
    clearCandidatesQueue(calleeId);

    function onError (callerReason, calleeReason) {
        if (pipeline) pipeline.release();
        if (caller) {
            var callerMessage = {
                id: 'callResponse',
                response: 'rejected'
            }
            if (callerReason) callerMessage.message = callerReason;
            caller.sendMessage(callerMessage);
        }

        var calleeMessage = {
            id: 'stopCommunication'
        };
        if (calleeReason) calleeMessage.message = calleeReason;
        callee.sendMessage(calleeMessage);

        sendLogMessage(caller || callee, {json_data: {
            msg: 'pipeline error',
            data: {
                pipeline: pipeline && pipeline.pipeline ? pipeline.pipeline.id : 'unknown',
                reason: callerReason || calleeReason || 'unknown'
            }
        }});
    }

    var callee = userRegistry.getById(calleeId);
    if (!from || !userRegistry.getByName(from)) {
        return onError(null, 'unknown from = ' + from);
    }
    var caller = userRegistry.getByName(from);

    if (callResponse !== 'accept') {
        caller.sendMessage({id: 'callResponse', response: 'rejected', message: 'user declined'});
        return undefined;
    }

    if (caller && pipelines[caller.id]) {
        stop(caller.id);
    }
    if (callee && pipelines[callee.id]) {
        stop(callee.id);
    }

    var pipeline = new CallMediaPipeline();
    pipelines[caller.id] = pipeline;
    pipelines[callee.id] = pipeline;

    pipeline.createPipeline(caller.id, callee.id, caller.bandwidth, ws, function (error) {
        if (error) {
            return onError(error, error);
        }

        pipeline.generateSdpAnswer(caller.id, caller.sdpOffer, function (error, callerSdpAnswer) {
            if (error) {
                return onError(error, error);
            }

            pipeline.generateSdpAnswer(callee.id, calleeSdp, function (error, calleeSdpAnswer) {
                if (error) {
                    return onError(error, error);
                }

                callee.sendMessage({id: 'startCommunication', sdpAnswer: calleeSdpAnswer});
                caller.sendMessage({id: 'callResponse',response: 'accepted', sdpAnswer: callerSdpAnswer});

                sendLogMessage(caller, {json_data: {
                    msg: 'pipeline created',
                    data: {pipeline: pipeline && pipeline.pipeline ? pipeline.pipeline.id : 'unknown'}
                }});
            });
        });
    });
}

function call (callerId, to, from, sdpOffer, bandwidth) {
    clearCandidatesQueue(callerId);

    var caller = userRegistry.getById(callerId);
    var callee = userRegistry.getByName(to);

    if (!caller) {
        // if caller has disconnected
        return undefined;
    }

    if (!callee) {
        caller.sendMessage({id: 'callResponse', response: 'rejected', message: 'User ' + to + ' is not registered'});
        return undefined;
    }

    caller.sdpOffer = sdpOffer;
    caller.bandwidth = bandwidth;
    callee.peer = from;
    caller.peer = to;

    if (pipelines[caller.id]) {
        clearCandidatesQueue(caller.id);
        stop(caller.id);
    }
    if (pipelines[callee.id]) {
        clearCandidatesQueue(callee.id);
        stop(callee.id);
    }

    try {
        callee.sendMessage({id: 'incomingCall', from: from});
    } catch (exception) {
        caller.sendMessage({id: 'callResponse', response: 'rejected', message: 'Error ' + exception});
        sendLogMessage(caller, {json_data: {
            msg: 'start call error',
            data: {error: exception}
        }});
    }
}

function loopbackCall (id, sdpOffer) {
    clearCandidatesQueue(id);

    function onError (callerReason, calleeReason) {
        if (pipeline) pipeline.release();
    }

    var user = userRegistry.getById(id);

    if (!user) {
        // if user has disconnected
        return undefined;
    }

    var pipeline = new LoopbackCallMediaPipeline();
    pipelines[id] = pipeline;

    pipeline.createPipeline(user.id, ws, function (error) {
        if (error) {
            return onError(error, error);
        }

        pipeline.generateSdpAnswer(user.id, sdpOffer, function (error, callerSdpAnswer) {
            if (error) {
                return onError(error, error);
            }

            user.sendMessage({id: 'loopbackCallResponse', response: 'accepted', sdpAnswer: callerSdpAnswer});

            sendLogMessage(user, {json_data: {
                msg: 'pipeline.test created',
                data: {pipeline: pipeline && pipeline.pipeline ? pipeline.pipeline.id : 'unknown'}
            }});
        });
    });
}

function register (id, name, callee, opts, ws, callback) {
    function onError (error) {
        sendMessage(ws, {id: 'registerResponse', response: 'rejected', message: error});
        sendLogMessage({
            opts: opts
        }, {json_data: {
            msg: 'kms user register error',
            data: {id: id, name: name, error: error}
        }});
    }

    if (!name) {
        return onError('empty user name');
    }

    var user = userRegistry.getByName(name);
    if (user) {
        user.sendMessage({id: 'unregister'});
        stop(user.id);
        userRegistry.unregister(user.id);
        sendLogMessage(user, {json_data: {
            msg: 'kms user unregistered'
        }});
    }

    user = new UserSession(id, name, callee, opts, ws);
    userRegistry.register(user);
    try {
        sendMessage(ws, {id: 'registerResponse', response: 'accepted'});
    } catch (exception) {
        return onError(exception);
    }

    sendLogMessage(user, {json_data: {
        msg: 'kms user registered',
        data: {sessionId: id}
    }});

    var userCaller = userRegistry.getByCallee(name);
    if (userCaller) {
        userCaller.sendMessage({id: 'calCalleeReady'});
    }

    if (callee) {
        var userCallee = userRegistry.getByName(callee);
        if (userCallee) {
            user.sendMessage({id: 'calCalleeReady'});
        }
    }
}

function clearCandidatesQueue (sessionId) {
    if (candidatesQueue[sessionId]) {
        delete candidatesQueue[sessionId];
    }
}

function onIceCandidate (sessionId, _candidate) {
    var candidate = kurento.getComplexType('IceCandidate')(_candidate);
    var user = userRegistry.getById(sessionId);
    if (user && pipelines[user.id] && pipelines[user.id].webRtcEndpoint && pipelines[user.id].webRtcEndpoint[user.id]) {
        var webRtcEndpoint = pipelines[user.id].webRtcEndpoint[user.id];
        webRtcEndpoint.addIceCandidate(candidate);
    } else if (user) {
        if (!candidatesQueue[user.id]) {
            candidatesQueue[user.id] = [];
        }
        candidatesQueue[user.id].push(candidate);
    }
}

function processMessage (sessionId, ws, message) {
    switch (message.id) {
        case 'noop':
            break;

        case 'status':
            status(sessionId, ws);
            break;

        case 'register':
            register(sessionId, message.name, message.callee, message.opts, ws);
            break;

        case 'call':
            call(sessionId, message.to, message.from, message.sdpOffer, message.bandwidth);

            sendLogMessage(sessionId, {json_data: {
                msg: 'start call',
                data: {bandwidth: message.bandwidth}
            }});
            break;

        case 'incomingCallResponse':
            incomingCallResponse(sessionId, message.from, message.callResponse, message.sdpOffer, ws);

            sendLogMessage(sessionId, {json_data: {
                msg: 'incoming call'
            }});
            break;

        case 'loopbackCall':
            loopbackCall(sessionId, message.sdpOffer);
            break;

        case 'stop':
            stop(sessionId);

            sendLogMessage(sessionId, {json_data: {
                msg: 'stop call'
            }});
            break;

        case 'onIceCandidate':
            onIceCandidate(sessionId, message.candidate);
            break;

        default:
            sendMessage(ws, {id: 'error', message: 'Invalid message ' + message});
            break;
    }
}

/*
 * Server startup
 */

var app = express();
var port = process.env.PORT || argv.port;
var server = argv.ssl ? https.createServer(options, app) : http.createServer(app);

server.listen(port, function () {
    console.log('Kurento proxy started');
    console.log('Open http' + (argv.ssl ? 's' : '') + '://' + options.hostname + ':' + port + ' with a WebRTC capable browser');
    console.log('Kurento server ' + argv.ws_uri);
    console.log('Records folder ' + argv.records_path);
});

var io = socketio(server, {
    path: '/socket.io',
    pingInterval: 5 * 1000,
    pingTimeout: 10 * 1000
});

io.on('connection', function (ws) {
    ws.$$$socketio = true;
    var sessionId = nextUniqueId();
    console.log('Connection received with sessionId ' + sessionId);

    ws.on('error', function (error) {
        console.log('Connection ' + sessionId + ' error');

        stop(sessionId);

        sendLogMessage(sessionId, {json_data: {
            msg: 'kms user ws error',
            data: {sessionId: sessionId, error: error.toString()}
        }});
    });

    ws.on('disconnect', function () {
        console.log('Connection ' + sessionId + ' closed');

        var user = userRegistry.getById(sessionId);
        stop(sessionId);
        userRegistry.unregister(sessionId);

        sendLogMessage(user, {json_data: {
            msg: 'kms user unregistered',
            data: {sessionId: sessionId}
        }});
    });

    ws.on('message', function (message) {
        //console.log('Connection ' + sessionId + ' received message', message);

        processMessage(sessionId, ws, message);
    });
});

var wss = new ws.Server({
    server: server,
    path: '/one2one'
});

wss.on('connection', function (ws) {
    var sessionId = nextUniqueId();
    console.log('Connection received with sessionId ' + sessionId);

    ws.on('error', function (error) {
        console.log('Connection ' + sessionId + ' error');
        stop(sessionId);

        sendLogMessage(sessionId, {json_data: {
            msg: 'kms user ws error',
            data: {sessionId: sessionId, error: error.toString()}
        }});
    });

    ws.on('close', function () {
        console.log('Connection ' + sessionId + ' closed');
        var user = userRegistry.getById(sessionId);
        stop(sessionId);
        userRegistry.unregister(sessionId);

        sendLogMessage(user, {json_data: {
            msg: 'kms user unregistered',
            data: {sessionId: sessionId}
        }});
    });

    ws.on('message', function (_message) {
        var message = JSON.parse(_message);
        console.log('Connection ' + sessionId + ' received message ', message);

        processMessage(sessionId, ws, message);
    });
});

app.use(express.static(path.join(__dirname, 'static')));
