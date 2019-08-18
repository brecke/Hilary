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

// import mkdirp from 'mkdirp';
import { ensureDir } from 'fs-extra';

import { logger } from 'oae-logger';

import * as Cleaner from 'oae-util/lib/cleaner';
import * as PreviewAPI from './api';
// eslint-disable-next-line no-unused-vars, import/namespace
import * as activity from './activity';

const log = logger('oae-preview-processor');

/**
 * Starts listening for new pieces of content that should be handled.
 */
export function init(config, callback) {
  // Create the previews directory and periodically clean it.
  // mkdirp does not throw an error if the directory already exist
  // so there is no need to check that first.
  return ensureDir(config.previews.tmpDir)
    .then(() => {
      // Periodically clean that directory.
      Cleaner.start(config.previews.tmpDir, config.files.cleaner.interval);
      return PreviewAPI.refreshPreviewConfiguration(config);
    })
    .then(() => {
      return callback();
    })
    .catch(error => {
      const err = new Error('Could not create the previews directory');
      err.code = 500;
      log().error({ err: error }, err.message);
      return Promise.reject(err);
      // throw err;
    });
}
