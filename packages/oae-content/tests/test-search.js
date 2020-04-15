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
import assert from 'assert';
import fs from 'fs';
import path from 'path';
import _ from 'underscore';

import * as Cassandra from 'oae-util/lib/cassandra';
import * as ConfigTestUtil from 'oae-config/lib/test/util';
import * as ElasticSearch from 'oae-search/lib/internal/elasticsearch';
import * as MQTestUtil from 'oae-util/lib/test/mq-util';
import * as PreviewAPI from 'oae-preview-processor/lib/api';
import PreviewConstants from 'oae-preview-processor/lib/constants';
import * as PreviewTestUtil from 'oae-preview-processor/lib/test/util';
import * as RestAPI from 'oae-rest';
import * as SearchAPI from 'oae-search';
import * as SearchTestsUtil from 'oae-search/lib/test/util';
import * as TestsUtil from 'oae-tests';

const PUBLIC = 'public';

describe('Search', () => {
  // REST contexts we can use to do REST requests
  let anonymousRestContext = null;
  let camAdminRestContext = null;
  let gtAdminRestContext = null;
  let signedAdminRestContext = null;
  let globalAdminRestContext = null;

  before(callback => {
    anonymousRestContext = TestsUtil.createTenantRestContext(global.oaeTests.tenants.cam.host);
    camAdminRestContext = TestsUtil.createTenantAdminRestContext(global.oaeTests.tenants.cam.host);
    gtAdminRestContext = TestsUtil.createTenantAdminRestContext(global.oaeTests.tenants.gt.host);
    signedAdminRestContext = TestsUtil.createTenantAdminRestContext(global.oaeTests.tenants.localhost.host);
    globalAdminRestContext = TestsUtil.createGlobalAdminRestContext();
    return callback();
  });

  after(callback => {
    PreviewAPI.disable(err => {
      assert.ok(!err);

      ConfigTestUtil.updateConfigAndWait(
        globalAdminRestContext,
        'admin',
        { 'oae-content/storage/backend': 'local' },
        err => {
          assert.ok(!err);
          return callback();
        }
      );
    });
  });

  /**
   * Purge the preview processor queue and enable the Preview Processor
   *
   * @param  {Function}   callback    Standard callback function
   */
  const _purgeAndEnable = function(callback) {
    // Purge anything that is hanging around in the preview processing queues
    PreviewTestUtil.purgePreviewsQueue(() => {
      PreviewTestUtil.purgeRegeneratePreviewsQueue(() => {
        PreviewTestUtil.purgeFoldersPreviewsQueue(() => {
          // Enable the Preview Processor
          PreviewAPI.enable(err => {
            assert.ok(!err);

            return callback();
          });
        });
      });
    });
  };

  /*!
   * Get the document with the specified id from the search results.
   *
   * @param  {SearchResult}  results     The search results object
   * @param  {String}        docId       The id of the document to search
   * @return {Object}                    The search document. `null` if it didn't exist
   */
  const _getDocById = function(results, docId) {
    for (let i = 0; i < results.results.length; i++) {
      const doc = results.results[i];
      if (doc.id === docId) {
        return doc;
      }
    }

    return null;
  };

  /*!
   * Creates a file and waits till it has been preview processed.
   *
   * @param  {Stream}      stream     The stream that points to the file that should be uploaded.
   * @param  {Function}    callback   Standard callback method that gets called when the file has previews associated to it.
   */
  const _createContentAndWait = function(stream, callback) {
    // When the queue is empty, we create a piece of content for which we can generate preview items.
    MQTestUtil.whenTasksEmpty(PreviewConstants.MQ.TASK_GENERATE_PREVIEWS, () => {
      MQTestUtil.whenTasksEmpty(PreviewConstants.MQ.TASK_GENERATE_PREVIEWS_PROCESSING, () => {
        TestsUtil.generateTestUsers(signedAdminRestContext, 1, (err, response, creator) => {
          assert.ok(!err);

          RestAPI.Content.createFile(
            creator.restContext,
            {
              displayName: 'Test Content 1',
              description: 'Test content description 1',
              visibility: PUBLIC,
              file: stream,
              managers: [],
              viewers: [],
              folders: []
            },
            (err, contentObj) => {
              assert.ok(!err);

              // Wait till the PP items have been generated
              MQTestUtil.whenTasksEmpty(PreviewConstants.MQ.TASK_GENERATE_PREVIEWS, () => {
                MQTestUtil.whenTasksEmpty(PreviewConstants.MQ.TASK_GENERATE_PREVIEWS_PROCESSING, () => {
                  // Ensure the preview items are there
                  RestAPI.Content.getContent(creator.restContext, contentObj.id, (err, updatedContent) => {
                    assert.ok(!err);
                    assert.ok(updatedContent.previews);
                    assert.strictEqual(updatedContent.previews.status, 'done');
                    assert.strictEqual(updatedContent.previews.pageCount, 1);

                    return callback(creator, updatedContent);
                  });
                });
              });
            }
          );
        });
      });
    });
  };

  describe('Indexing', () => {
    /**
     * Test that verifies when a content item is indexed with just the content id, it still indexes the content
     * item.
     */
    it('verify indexing without full content item', callback => {
      TestsUtil.generateTestUsers(camAdminRestContext, 1, (err, users, doer) => {
        assert.ok(!err);

        RestAPI.Content.createLink(
          doer.restContext,
          'test-search index-without-full-content-item',
          'Test content description 1',
          'public',
          'http://www.oaeproject.org/',
          [],
          [],
          [],
          (err, link) => {
            assert.ok(!err);

            // Verify the content item exists
            SearchTestsUtil.searchAll(
              doer.restContext,
              'general',
              null,
              { resourceTypes: 'content', q: 'index-without-full-content-item' },
              (err, results) => {
                assert.ok(!err);
                const contentDoc = _getDocById(results, link.id);
                assert.ok(contentDoc);

                // Delete the content item from the index under the hood, this is to avoid the automatic index events invalidating the test
                ElasticSearch.del('resource', link.id, err => {
                  assert.ok(!err);

                  // Verify the content item no longer exists
                  SearchTestsUtil.searchAll(
                    doer.restContext,
                    'general',
                    null,
                    { resourceTypes: 'content', q: 'index-without-full-content-item' },
                    (err, results) => {
                      assert.ok(!err);
                      const contentDoc = _getDocById(results, link.id);
                      assert.ok(!contentDoc);

                      // Fire off an indexing task using just the content id
                      SearchAPI.postIndexTask('content', [{ id: link.id }], { resource: true }, err => {
                        assert.ok(!err);

                        // Ensure that the full content item is now back in the search index
                        SearchTestsUtil.searchAll(
                          doer.restContext,
                          'general',
                          null,
                          { resourceTypes: 'content', q: 'index-without-full-content-item' },
                          (err, results) => {
                            assert.ok(!err);
                            const contentDoc = _getDocById(results, link.id);
                            assert.ok(contentDoc);

                            // Ensure that the full tenant object is passed back.
                            assert.ok(_.isObject(contentDoc.tenant));
                            assert.ok(contentDoc.tenant.displayName);
                            assert.ok(contentDoc.tenant.alias);
                            return callback();
                          }
                        );
                      });
                    }
                  );
                });
              }
            );
          }
        );
      });
    });

    /**
     * Verify that the mime property only returns on search results of type 'file'.
     */
    it('verify mime type', callback => {
      TestsUtil.generateTestUsers(camAdminRestContext, 1, (err, users, doer) => {
        assert.ok(!err);

        // Make sure links don't have mime field
        let description = TestsUtil.generateTestUserId('mimetype-test');
        RestAPI.Content.createLink(
          doer.restContext,
          'Test Content 1',
          description,
          'public',
          'http://www.oaeproject.org/',
          [],
          [],
          [],
          (err, link) => {
            assert.ok(!err);

            SearchTestsUtil.searchAll(
              doer.restContext,
              'general',
              null,
              { resourceTypes: 'content', q: description },
              (err, results) => {
                assert.ok(!err);
                const contentDoc = _getDocById(results, link.id);
                assert.ok(contentDoc);
                assert.ok(!contentDoc.mime);

                // Make sure files do get mime field
                const file = fs.createReadStream(path.join(__dirname, '/data/oae-video.png'));
                description = TestsUtil.generateTestUserId('mimetype-test');
                RestAPI.Content.createFile(
                  doer.restContext,
                  {
                    displayName: 'Test Content 2',
                    description,
                    visiblity: PUBLIC,
                    file,
                    managers: [],
                    viewers: [],
                    folders: []
                  },
                  (err, contentObj) => {
                    assert.ok(!err);
                    assert.ok(contentObj);
                    assert.strictEqual(contentObj.mime, 'image/png');

                    SearchTestsUtil.searchAll(
                      doer.restContext,
                      'general',
                      null,
                      { resourceTypes: 'content', q: description },
                      (err, results) => {
                        assert.ok(!err);
                        const contentDoc = _getDocById(results, contentObj.id);
                        assert.ok(contentDoc);
                        assert.strictEqual(contentDoc.mime, 'image/png');

                        return callback();
                      }
                    );
                  }
                );
              }
            );
          }
        );
      });
    });

    /**
     * Test that verifies that PDF files are indexed
     */
    it('verify full-text indexing of pdf', callback => {
      if (!PreviewAPI.getConfiguration().previews.enabled) {
        return callback();
      }

      _purgeAndEnable(() => {
        ConfigTestUtil.updateConfigAndWait(
          globalAdminRestContext,
          'admin',
          { 'oae-content/storage/backend': 'test' },
          err => {
            assert.ok(!err);

            const pdfStream = function() {
              return fs.createReadStream(path.join(__dirname, '/data/test.pdf'));
            };

            _createContentAndWait(pdfStream, (creator, content) => {
              // Verify we can find the content from the PDF in general searches
              SearchTestsUtil.searchAll(
                creator.restContext,
                'general',
                null,
                { resourceTypes: 'content', q: 'b4c3f09e74f58b0aeee34d9c3cd9333a' },
                (err, results) => {
                  assert.ok(!err);
                  assert.ok(_getDocById(results, content.id));

                  // Verify we can find the content from the PDF in library searches
                  SearchTestsUtil.searchAll(
                    creator.restContext,
                    'content-library',
                    [creator.user.id],
                    { q: 'b4c3f09e74f58b0aeee34d9c3cd9333a' },
                    (err, results) => {
                      assert.ok(!err);
                      assert.ok(_getDocById(results, content.id));
                      return callback();
                    }
                  );
                }
              );
            });
          }
        );
      });
    });

    /**
     * Test that verifies that a revision without a plain.txt file still gets indexed
     */
    it('verify a revision without a plain.txt file still gets indexed', callback => {
      if (!PreviewAPI.getConfiguration().previews.enabled) {
        return callback();
      }

      _purgeAndEnable(() => {
        ConfigTestUtil.updateConfigAndWait(
          globalAdminRestContext,
          'admin',
          { 'oae-content/storage/backend': 'test' },
          err => {
            assert.ok(!err);

            const pdfStream = function() {
              return fs.createReadStream(path.join(__dirname, '/data/test.pdf'));
            };

            _createContentAndWait(pdfStream, (creator, content) => {
              // Verify we can find the content from the PDF in general searches
              SearchTestsUtil.searchAll(
                creator.restContext,
                'general',
                null,
                { resourceTypes: 'content', q: 'b4c3f09e74f58b0aeee34d9c3cd9333a' },
                (err, results) => {
                  assert.ok(!err);
                  assert.ok(_getDocById(results, content.id));

                  // Now, for whatever reason might not've been able to generate a plain.txt file,
                  // or the record got lost, or .. The search re-index should not get stalled
                  // by this fact
                  Cassandra.runQuery(
                    'DELETE FROM "PreviewItems" WHERE "revisionId" = ?',
                    [content.latestRevisionId],
                    err => {
                      assert.ok(!err);

                      // Drop all the data
                      SearchTestsUtil.deleteAll(() => {
                        // Re-index everything
                        SearchTestsUtil.reindexAll(globalAdminRestContext, () => {
                          // Assert we can no longer find the document by its content
                          SearchTestsUtil.searchAll(
                            creator.restContext,
                            'general',
                            null,
                            { resourceTypes: 'content', q: 'b4c3f09e74f58b0aeee34d9c3cd9333a' },
                            (err, results) => {
                              assert.ok(!err);
                              assert.ok(!_getDocById(results, content.id));

                              // Assert we can find it by its name however
                              SearchTestsUtil.searchAll(
                                creator.restContext,
                                'general',
                                null,
                                { resourceTypes: 'content', q: content.displayName },
                                (err, results) => {
                                  assert.ok(!err);
                                  assert.ok(_getDocById(results, content.id));

                                  return callback();
                                }
                              );
                            }
                          );
                        });
                      });
                    }
                  );
                }
              );
            });
          }
        );
      });
    });
  });
});
