/*!
 * Copyright 2014 Apereo Foundation (AF) Licensed under the
 * Educational Community License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 *     http://opensource.org/licenses/ECL-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

var log = require('oae-logger').logger('meetups-api');
var PrincipalsAPI = require('oae-principals/lib/api');
var Validator = require('oae-authz/lib/validator').Validator;

var MeetupsAPI = require('oae-meetups');
var MeetupsConfig = require('oae-config').config('oae-meetups');
var MeetupsConstants = require('./constants').MeetupsConstants;
var MeetupsDAO = require('./internal/dao');

var Config = require('oae-config').config('oae-meetups');
var BBBProxy = require('./internal/proxy');
var ContentAPI = require('oae-content/lib/api');
var DOMParser = require('xmldom').DOMParser;
var XMLSerializer = require('xmldom').XMLSerializer;
var xpath = require('xpath');
var jwt = require('jsonwebtoken');

// TODO jsdoc
var joinMeetup = function(ctx, groupId, callback) {
    var validator = new Validator();
    validator.check(null, {'code': 401, 'msg': 'Only authenticated users can join meetups'}).isLoggedInUser(ctx);
    validator.check(groupId, {'code': 400, 'msg': 'Invalid groupId id provided'}).isResourceId();

    if (validator.hasErrors()) {
        return callback(validator.getFirstError());
    }

    PrincipalsAPI.getFullGroupProfile(ctx, groupId, function(err, groupProfile) {
        if (err) {
            return callback(err);
        }

        var profile = (JSON.parse(JSON.stringify(groupProfile)));

        MeetupsAPI.Bbb.getDefaultConfigXML(ctx, function(err, result) {
            if(err || result.returncode !== 'success') {
                return callback({'code': 503, 'msg': 'Fatal error'});
            }

            defaultConfigXML = result.defaultConfigXML;
            var serializer = new XMLSerializer();
            var doc = new DOMParser().parseFromString(defaultConfigXML);
            var select = xpath.useNamespaces();
            var node;

            // set layout bbb.layout.name.videochat and others
            node = select('//layout ', doc, true);
            node.setAttribute('defaultLayout', 'bbb.layout.name.videochat');
            node.setAttribute('showLayoutTools', 'false');
            node.setAttribute('confirmLogout', 'false');
            node.setAttribute('showRecordingNotification', 'false');
            // process modules
            // remove desktop sharing
            node = xpath.select1("//modules/module[@name=\'ScreenshareModule\']", doc);
            node.setAttribute('showButton', 'false');
            // remove PhoneModule button
            node = xpath.select1("//modules/module[@name=\'PhoneModule\']", doc);
            node.setAttribute('showButton', 'true');
            node.setAttribute('skipCheck', 'true');
            node.setAttribute('listenOnlyMode', 'false');
            // remove VideoconfModule button
            node = xpath.select1("//modules/module[@name=\'VideoconfModule\']", doc);
            node.setAttribute('showButton', 'true');
            node.setAttribute('autoStart', 'true');
            node.setAttribute('skipCamSettingsCheck', 'true');
            // remove layout menu
            node = xpath.select1("//modules/module[@name=\'LayoutModule\']", doc);
            node.setAttribute('enableEdit', 'false');

            var xml = serializer.serializeToString(doc);
            MeetupsAPI.Bbb.joinURL(ctx, profile, xml, function(err, joinInfo) {
                if(err) {
                    //res.send(503, 'Fatal error'); *res is not defined here...*
                    log().info('Fatal error');
                }

                MeetupsAPI.emit(MeetupsConstants.events.JOIN_MEETUP, ctx, groupProfile, function(errs) {

                });

                return callback(null, joinInfo);
            });
        });
    });
};

// TODO jsdoc
var isMeetingRunning = function (ctx, groupId, callback) {
    var validator = new Validator();
    validator.check(null, {'code': 401, 'msg': 'Only authenticated users can end meetups'}).isLoggedInUser(ctx);
    validator.check(groupId, {'code': 400, 'msg': 'Invalid groupId id provided'}).isResourceId();

    PrincipalsAPI.getFullGroupProfile(ctx, groupId, function(err, groupProfile) {
        if(err) {
            callback(err);
        }

        // TODO this is not quite right, this could come from the activity stream
        // outside of a group, in which case this ajax call shouldn't be happening
        var profile;
        try {
            profile = (JSON.parse(JSON.stringify(groupProfile)));
        } catch (error) {
            log().info('Unable to fetch info about group profile');
            callback(null);
        }

        MeetupsAPI.Bbb.isMeetingRunning(ctx, profile, function(err, meetingInfo) {
            if(err) {
                callback(err);
            }

            callback(null, meetingInfo.running);
        });
    });

};

// TODO jsdoc
var endMeetup = function(ctx, groupId, callback) {
    var validator = new Validator();
    validator.check(null, {'code': 401, 'msg': 'Only authenticated users can end meetups'}).isLoggedInUser(ctx);
    validator.check(groupId, {'code': 400, 'msg': 'Invalid groupId id provided'}).isResourceId();

    PrincipalsAPI.getFullGroupProfile(ctx, groupId, function(err, groupProfile) {
        var profile = (JSON.parse(JSON.stringify(groupProfile)));
        MeetupsAPI.Bbb.getEndURL(ctx, profile, function(err, meetingInfo) {
            if(err) {
                callback(err);
            }
            MeetupsAPI.Bbb.endMeetup(meetingInfo, function(err) {
                if(err) {
                    callback(err);
                }
                return callback(null);
            });
        });
    });
};

// TODO jsdoc
var createRecordingLink = function(ctx, groupId, signed_parameters, callback) {
    var validator = new Validator();
    validator.check(groupId, {'code': 400, 'msg': 'Invalid groupId id provided'}).isResourceId();

    if (validator.hasErrors()) {
        return callback(validator.getFirstError());
    }

    var secret = Config.getValue(ctx.tenant().alias, 'bbb', 'secret');

    jwt.verify(signed_parameters, secret, {algorithms: ["HS256"]},function(err, decoded) {
        if (err) {
            return callback({'code': 401, 'msg': ''});
        }

        PrincipalsAPI.getFullGroupProfile(ctx, groupId, function(err, groupProfile) {
            if (err) {
                return callback(err);
            }

            MeetupsAPI.Bbb.getRecordingsURL(ctx, decoded, function(err, getRecordings) {
                ctx = getRecordings.ctx;

                MeetupsAPI.Bbb.createRecording(ctx, getRecordings, function(err) {
                    if(err) {
                        return callback(err);
                    }

                    return callback(null);
                });
            });
        });
    });
};

// TODO jsdoc
var deleteRecordingLink = function(ctx, recordingID, callback) {

    // debug
    console.log("......................................");
    console.dir("I am creating a delete recording link!");

    MeetupsAPI.Bbb.getDeleteRecordingsURL(ctx, recordingID, function(err, deleteRecordings) {
        if(err) {
            callback(err);
        }

        MeetupsAPI.Bbb.deleteRecording(deleteRecordings, function(err) {
            if(err) {
                return callback(err);
            }
            return callback(null);
        });
    });
};

module.exports = {
    'createRecordingLink': createRecordingLink,
    'deleteRecordingLink': deleteRecordingLink,
    'joinMeetup': joinMeetup,
    'endMeetup': endMeetup,
    'isMeetingRunning': isMeetingRunning
};
