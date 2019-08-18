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

import PreviewConstants from 'oae-preview-processor/lib/constants';

import * as MQ from 'oae-util/lib/mq';
import * as TaskQueue from 'oae-util/lib/taskqueue';

/**
 * Purges all the events out of the previews queue
 *
 * @param  {Function}       callback        Standard callback function
 * @throws {AssertionError}                 Thrown if an error occurs while purging the queue
 */
const purgePreviewsQueue = async function() {
  try {
    // Unbind the old listener (if any) and bind a new one, so we can purge the queue
    await TaskQueue.unbind(PreviewConstants.MQ.TASK_GENERATE_PREVIEWS);

    await TaskQueue.bind(
      PreviewConstants.MQ.TASK_GENERATE_PREVIEWS,
      () => {
        return new Promise(resolve => {
          resolve();
        });
      },
      { subscribe: { subscribe: false } }
    );

    // Purge anything that is in the queue
    await MQ.purge(PreviewConstants.MQ.TASK_GENERATE_PREVIEWS);

    // Unbind our dummy-handler from the queue
    await TaskQueue.unbind(PreviewConstants.MQ.TASK_GENERATE_PREVIEWS);
  } catch (error) {
    throw error;
  }
};

/**
 * Purges all the messages out of the regenerate previews queue
 *
 * @param  {Function}       callback        Standard callback function
 * @throws {AssertionError}                 Thrown if an error occurs while purging the queue
 */
const purgeRegeneratePreviewsQueue = async function() {
  try {
    // Unbind the old listener (if any) and bind a new one, so we can purge the queue
    await TaskQueue.unbind(PreviewConstants.MQ.TASK_REGENERATE_PREVIEWS);

    await TaskQueue.bind(
      PreviewConstants.MQ.TASK_REGENERATE_PREVIEWS,
      () => {
        return new Promise(resolve => {
          resolve();
        });
      },
      { subscribe: { subscribe: false } }
    );

    // Purge anything that is in the queue
    await MQ.purge(PreviewConstants.MQ.TASK_REGENERATE_PREVIEWS);

    // Unbind our dummy-handler from the queue
    await TaskQueue.unbind(PreviewConstants.MQ.TASK_REGENERATE_PREVIEWS);
  } catch (error) {
    throw error;
  }
};

/**
 * Purges all the messages out of the generate folder previews queue
 *
 * @param  {Function}       callback        Standard callback function
 * @throws {AssertionError}                 Thrown if an error occurs while purging the queue
 */
const purgeFoldersPreviewsQueue = async function() {
  try {
    // Unbind the old listener (if any) and bind a new one, so we can purge the queue
    await TaskQueue.unbind(PreviewConstants.MQ.TASK_GENERATE_FOLDER_PREVIEWS);

    await TaskQueue.bind(
      PreviewConstants.MQ.TASK_GENERATE_FOLDER_PREVIEWS,
      () => {
        return new Promise(resolve => {
          resolve();
        });
      },
      {
        subscribe: { subscribe: false }
      }
    );

    // Purge anything that is in the queue
    await MQ.purge(PreviewConstants.MQ.TASK_GENERATE_FOLDER_PREVIEWS);

    // Unbind our dummy-handler from the queue
    await TaskQueue.unbind(PreviewConstants.MQ.TASK_GENERATE_FOLDER_PREVIEWS);
  } catch (error) {
    throw error;
  }
};

export { purgePreviewsQueue, purgeRegeneratePreviewsQueue, purgeFoldersPreviewsQueue };
