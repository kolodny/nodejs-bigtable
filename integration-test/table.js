'use strict';

const Bigtable = require('../');

const assert = require('assert');
const grpc = require('grpc');
const sinon = require('sinon');
const through = require('through2');

function chainEmits(emitter, responses) {
  var index = 0;
  setImmediate(next);
  return emitter;

  function next() {
    if (index < responses.length) {
      const response = responses[index];
      const name = response[0];
      const data = response[1];
      index++;
      emitter.emit(name, data);;
      setImmediate(next);
    }
  }
}

function entryResponses(statusCodes) {
  return {
    entries: statusCodes.map((code, index) => ({
      index,
      status: { code },
    }))
  };
}

describe('Bigtable/Table', () => {
  const bigtable = new Bigtable();
  bigtable.grpcCredentials = grpc.credentials.createInsecure();
  const bigtableAdminService = bigtable.getService_({
    service: 'BigtableTableAdmin',
  });
  const bigtableService = bigtable.getService_({service: 'Bigtable'});

  const INSTANCE = bigtable.instance('instance');
  const TABLE = INSTANCE.table('table');

  describe('mutate()', () => {
    let stub;
    let statusCodes;
    let responses;
    beforeEach(() => {
      responses = null;
      stub = sinon.stub(bigtableService, 'mutateRows', () => {
        // console.log('called stub');
        const response = through.obj();
        chainEmits(response, responses);
        return response;
      });
    });
    afterEach(() => {
      stub.restore();
    });

    it('has no error when the entries all have a response status code of zero', done => {
      responses = [
        ['metadata'],
        ['response', {code: 200}],
        ['data', entryResponses([0, 0, 0])],
        ['end'],
      ]
      const keys = ['foo', 'bar', 'baz'];
      const rowsToInsert = keys.map(key => ({
        key,
        data: {},
      }));
      TABLE.insert(rowsToInsert, done);
    });

    it('has a `PartialFailureError` error when an entry has a non zero response status code', done => {
      responses = [
        ['metadata'],
        ['response', {code: 200}],
        ['data', entryResponses([0, 1, 0])],
        ['end'],
      ]
      setTimeout(() => {
        responses = [
          ['metadata'],
          ['response', {code: 200}],
          ['data', entryResponses([1])],
          ['end'],
        ]
      }, 100)
      const keys = ['foo', 'bar', 'baz'];
      const rowsToInsert = keys.map(key => ({
        key,
        data: {},
      }));
      TABLE.insert(rowsToInsert, error => {
        assert(error.name);
        assert.equal(error.name, 'PartialFailureError');
        done();
      });
    });
  });
});