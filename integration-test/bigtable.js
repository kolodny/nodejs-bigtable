'use strict';

const Bigtable = require('../');

const assert = require('assert');
const grpc = require('grpc');
const sinon = require('sinon');
const through = require('through2');

describe('Bigtable', () => {
  const bigtable = new Bigtable();
  bigtable.grpcCredentials = grpc.credentials.createInsecure();
  const bigtableAdminService = bigtable.getService_({
    service: 'BigtableTableAdmin',
  });
  const bigtableService = bigtable.getService_({service: 'Bigtable'});

  const INSTANCE = bigtable.instance('instance');
  const TABLE = INSTANCE.table('table');

  describe('table', () => {
    describe('exists()', () => {
      let stub;
      let shouldExist;
      beforeEach(() => {
        stub = sinon.stub(bigtableAdminService, 'getTable', function() {
          const callback = arguments[arguments.length - 1];
          if (shouldExist) {
            // HTTP status code
            callback(null, {code: 200});
          } else {
            // GRPC status code
            callback({code: 5});
          }
        });
      });
      afterEach(() => {
        stub.restore();
      });

      it('can checks that a table exists', () => {
        shouldExist = true;
        return TABLE.exists().then(data => {
          const exists = data[0];
          assert.equal(exists, true);
        });
      });

      it('can checks that a table does not exists', () => {
        shouldExist = false;
        return TABLE.exists().then(data => {
          const exists = data[0];
          assert.equal(exists, false);
        });
      });
    });

    describe('mutate()', () => {
      let stub;
      let statusCodes;
      before(() => {
        stub = sinon.stub(bigtableService, 'mutateRows', function() {
          const response = through.obj();
          setImmediate(() => response.emit('metadata'));
          setImmediate(() => response.emit('response', {code: 200}));
          setImmediate(() =>
            response.emit('data', {
              entries: statusCodes.map(code => ({
                index: 0,
                status: { code },
              })),
            })
          );
          setImmediate(() => response.emit('end'));
          return response;
        });
      });
      after(() => {
        stub.restore();
      });

      it('has no error when the entries all have a response status code of zero', done => {
        const keys = ['foo', 'bar', 'baz'];
        const rowsToInsert = keys.map(key => ({
          key,
          data: {},
        }));
        statusCodes = [0, 0, 0];
        return TABLE.insert(rowsToInsert, done);
      });

      it('has a `PartialFailureError` error when an entry has a non zero response status code', done => {
        const keys = ['foo', 'bar', 'baz'];
        const rowsToInsert = keys.map(key => ({
          key,
          data: {},
        }));
        statusCodes = [0, 1, 0];
        return TABLE.insert(rowsToInsert, error => {
          assert.equal(error.name, 'PartialFailureError');
          done();
        });
      });
    });
  });
});
