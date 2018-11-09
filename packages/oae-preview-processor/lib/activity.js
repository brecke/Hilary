/*!
 * Copyright 2013 Apereo Foundation (AF) Licensed under the
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

const ActivityAPI = require('oae-activity');
const { ActivityConstants } = require('oae-activity/lib/constants');
const ActivityModel = require('oae-activity/lib/model');
const { Context } = require('oae-context');

const PreviewProcessorAPI = require('oae-preview-processor');
const PreviewConstants = require('./constants');

PreviewProcessorAPI.emitter.on(
  PreviewConstants.EVENTS.PREVIEWS_FINISHED,
  (content, revision, status) => {
    // Add the previews status.
    // The actual images will be added by the content activity entity transformer
    content.previews = content.previews || {};
    content.previews.status = status;

    const millis = Date.now();
    const actorResource = new ActivityModel.ActivitySeedResource('system', 'system', null);
    const objectResource = new ActivityModel.ActivitySeedResource('content', content.id, {
      content
    });
    const activitySeed = new ActivityModel.ActivitySeed(
      PreviewConstants.EVENTS.PREVIEWS_FINISHED,
      millis,
      ActivityConstants.verbs.CREATE,
      actorResource,
      objectResource
    );

    // Fake a request context
    const ctx = new Context(content.tenant);
    ActivityAPI.postActivity(ctx, activitySeed);
  }
);

ActivityAPI.registerActivityType(PreviewConstants.EVENTS.PREVIEWS_FINISHED, {
  groupBy: [{ actor: true }],
  streams: {
    activity: {
      router: {
        object: ['self']
      }
    }
  }
});

// Register a special entity type for activities that get generated by the system
ActivityAPI.registerActivityEntityType('system', {
  transformer: {
    internal(ctx, activityEntities, callback) {
      return callback(null, activityEntities);
    }
  },
  propagation(associationsCtx, entity, callback) {
    return callback(null, [{ type: ActivityConstants.entityPropagation.ALL }]);
  }
});
