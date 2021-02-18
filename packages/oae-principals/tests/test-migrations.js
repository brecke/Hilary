/*
 * Copyright 2016 Apereo Foundation (AF) Licensed under the
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

import { assert } from 'chai';
import _ from 'underscore';
import Chance from 'chance';

import { pluck, isEmpty, map, tail, head } from 'ramda';

import * as AuthzTestUtil from 'oae-authz/lib/test/util';
import * as Cassandra from 'oae-util/lib/cassandra';
import * as ContentTestUtil from 'oae-content/lib/test/util';
import * as Redis from 'oae-util/lib/redis';
import * as RestAPI from 'oae-rest';
import * as SearchTestUtil from 'oae-search/lib/test/util';
import * as TestsUtil from 'oae-tests';
import * as LowerCaseEmailsMigrator from '../../../etc/migration/12.3-to-12.4/lib/lower-case-emails.js';

const chance = new Chance();

describe('Principals Migration', () => {
  let globalAdminRestContext = null;
  let camAdminRestContext = null;

  before((callback) => {
    globalAdminRestContext = TestsUtil.createGlobalAdminRestContext();
    camAdminRestContext = TestsUtil.createTenantAdminRestContext(global.oaeTests.tenants.cam.host);
    return callback();
  });

  /*!
   * Randomly mix the case of the given string
   *
   * @param  {String}     toMix   The string whose case to mix
   * @return {String}             The string with its case mixed
   */
  const _mixCase = function (toMix) {
    return map((c) => {
      if (chance.bool()) {
        return c.toUpperCase();
      }

      return c.toLowerCase();
    }, toMix).join('');
  };

  /*!
   * Search for the email using email search, ensuring either their
   * presence or absense, depending on the `shouldContain` option
   *
   * @param  {String[]}       emails              The email addresses for which to search
   * @param  {Object}         opts                Execution options
   * @param  {Boolean}        opts.shouldContain  Whether or not the search results should contain an associated user
   * @param  {Function}       callback            Invoked when assertions are complete
   * @throws {AssertionError}                     Thrown if the assertions fail
   */
  const _assertAllEmailsSearch = function (emails, options, callback) {
    emails = emails.slice();
    if (isEmpty(emails)) {
      return callback();
    }

    const email = emails.shift();
    SearchTestUtil.assertSearchSucceeds(camAdminRestContext, 'email', null, { q: email }, (result) => {
      if (options.shouldContain) {
        assert.strictEqual(result.results.length, 1);
        assert.strictEqual(result.results[0].email, email.toLowerCase());
      } else {
        assert.strictEqual(result.results.length, 0);
      }

      return _assertAllEmailsSearch(emails, options, callback);
    });
  };

  /*!
   * Add the given emails to a content item, ensuring they are either added as
   * invitations or members, depending on if we expect the emails to be
   * found in the PrincipalsByEmail index.
   *
   * @param  {RestContext}    restContext         The REST context with which to share
   * @param  {String[]}       emails              The email addresses with which to share
   * @param  {Object}         opts                Execution options
   * @param  {Boolean}        opts.shouldInvite   Whether we should expect the share to result in email invitations
   * @param  {Function}       callback            Standard callback function
   * @throws {AssertionError}                     Thrown if the assertions fail
   */
  const _assertAllEmailsInvite = function (restContext, emails, options, callback) {
    ContentTestUtil.assertCreateLinkSucceeds(
      restContext,
      'google',
      'description',
      'public',
      'https://www.google.es',
      null,
      null,
      null,
      (link) => {
        // In this case, the share targets are the raw emails, so we should
        // expect the share to happen with just the emails. If there are matches
        // in the PrincipalsByEmail table, then they would be used as the target
        // principal and this operation would fail
        RestAPI.Content.shareContent(restContext, link.id, emails, (error) => {
          assert.notExists(error);
          AuthzTestUtil.assertGetInvitationsSucceeds(restContext, 'content', link.id, (invitations) => {
            ContentTestUtil.getAllContentMembers(restContext, link.id, null, (members) => {
              // Since invitations will be lower-cased, to compare
              // the arrays we should lower case our mixed-case local
              // copy
              const lowerCased = map((email) => {
                return email.toLowerCase();
              }, emails);

              if (options.shouldInvite) {
                // If we expected to invite them, ensure they are
                // all present in the invitations list
                assert.deepStrictEqual(pluck('email', invitations.results).sort(), lowerCased.sort());
                assert.lengthOf(members, 1);
              } else {
                // If we expected to add them (i.e., their emails
                // were found in the system), then verify they exist
                // as members
                assert.strictEqual(invitations.results.length, 0);
                assert.strictEqual(members.length, emails.length + 1);
              }

              return callback();
            });
          });
        });
      }
    );
  };

  /**
   * Test that verifies emails are made case insensitive
   */
  it('verify emails are made case insensitive', (callback) => {
    TestsUtil.generateTestUsers(camAdminRestContext, 51, (error, users) => {
      assert.notExists(error);

      const actor = head(users);
      const targets = tail(users);

      // Mix the case of all emails that were persisted in this group
      const userIds = _.chain(targets).pluck('user').pluck('id').value();
      const userIdEmails = _.chain(targets)
        .map((user) => {
          return [
            user.user.id,
            {
              before: user.user.email,
              after: _mixCase(user.user.email)
            }
          ];
        })
        .object()
        .value();
      const queries = _.chain(userIdEmails)
        .map((email, userId) => {
          return [
            {
              query: 'UPDATE "Principals" SET "email" = ? WHERE "principalId" = ?',
              parameters: [email.after, userId]
            },
            {
              query: 'DELETE FROM "PrincipalsByEmail" WHERE "email" = ?',
              parameters: [email.before]
            },
            {
              query: 'INSERT INTO "PrincipalsByEmail" ("email", "principalId") VALUES (?, ?)',
              parameters: [email.after, userId]
            }
          ];
        })
        .flatten()
        .value();

      // Persist the mixed case emails outside the scope of the API to
      // reproduce the migration test conditions (i.e., there were
      // mixed-case emails in the database at time of ugprade)
      Cassandra.runBatchQuery(queries, (error_) => {
        assert.notExists(error_);
        Cassandra.runQuery('SELECT "principalId", "email" FROM "Principals" WHERE "principalId" IN ?', [userIds], (
          error /* , rows */
        ) => {
          assert.notExists(error);

          // Update redis and search since we updated outside the
          // scope of the API
          Redis.flush((error_) => {
            assert.notExists(error_);
            SearchTestUtil.reindexAll(globalAdminRestContext, () => {
              // Determine which users we should not be able to find in search
              // because of mixed-cased emails
              const userIdEmailsWithUpperCase = _.chain(userIdEmails)
                .map((emailInfo, userId) => {
                  return [userId, emailInfo];
                })
                .filter((entries) => {
                  const emailInfo = entries[1];

                  /**
                   * Retain all emails that contain an upper-case character
                   * (excluding things that don't have uppercase variants like '@')
                   */
                  return _.some(emailInfo.after, (c) => {
                    return c !== c.toLowerCase() && c === c.toUpperCase();
                  });
                })
                .object()
                .value();
              const emailsWithUpperCase = _.chain(userIdEmailsWithUpperCase).values().pluck('after').value();

              // Ensure that none of the emails can be searched for or linked to when sharing a content
              // item. This is the basis for the migration
              _assertAllEmailsSearch(emailsWithUpperCase, { shouldContain: false }, () => {
                _assertAllEmailsInvite(actor.restContext, emailsWithUpperCase, { shouldInvite: true }, () => {
                  // Run the migration
                  LowerCaseEmailsMigrator.doMigration((error, stats) => {
                    assert.notExists(error);
                    assert.strictEqual(stats.nUpdated, emailsWithUpperCase.length);
                    assert.strictEqual(stats.nFailed, 0);

                    // Refresh the search index
                    SearchTestUtil.reindexAll(globalAdminRestContext, () => {
                      // Ensure the emails are now all searchable and can be matched
                      // when sharing
                      _assertAllEmailsSearch(emailsWithUpperCase, { shouldContain: true }, () => {
                        _assertAllEmailsInvite(actor.restContext, emailsWithUpperCase, { shouldInvite: false }, () => {
                          return callback();
                        });
                      });
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  });
});
