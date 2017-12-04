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
      index++;
      const name = response[0];
      const data = response[1];
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
    let clock;
    let entryKeyMutates;
    let responses;
    let stub;
    let timeout;

    beforeEach(() => {

      // There's some weird bug with tick() and runAll(), this works.
      const realTimeout = setTimeout;
      (function tickTheClock() {
        timeout = realTimeout(() => {
          clock.tick(1000, 10);
          tickTheClock();
        });
      })();

      clock = sinon.useFakeTimers();
      entryKeyMutates = [];
      responses = null;
      stub = sinon.stub(bigtableService, 'mutateRows', (grpcOpts) => {
        entryKeyMutates.push(grpcOpts.entries.map(entry => entry.rowKey.asciiSlice()));
        // console.log('called stub');
        const response = through.obj();
        chainEmits(response, responses.shift());
        return response;
      });
    });
    afterEach(() => {
      clock.restore();
      clearTimeout(timeout)
      stub.restore();
    });

    it('has no error when the entries all have a response status code of zero', (done) => {
      responses = [
        [
          ['response', {code: 200}],
          ['data', entryResponses([0, 0, 0])],
          ['end'],
        ],
      ]
      const keys = ['foo', 'bar', 'baz'];
      const rowsToInsert = keys.map(key => ({
        method: 'insert',
        key,
        data: {},
      }));
      TABLE.mutate(rowsToInsert, error => {
        assert.ifError(error);
        assert.deepEqual(entryKeyMutates, [['foo', 'bar', 'baz']]);
        done();
      });
    });

    it('retries the failed mutations', (done) => {
      responses = [
        [
          ['response', {code: 200}],
          ['data', entryResponses([0, 1, 1])],
          ['end'],
        ], [
          ['response', {code: 200}],
          ['data', entryResponses([1, 0])],
          ['end'],
        ], [
          ['response', {code: 200}],
          ['data', entryResponses([1])],
          ['end'],
        ], [
          ['response', {code: 200}],
          ['data', entryResponses([0])],
          ['end'],
        ]
      ];
      const keys = ['foo', 'bar', 'baz'];
      const rowsToInsert = keys.map(key => ({
        method: 'insert',
        key,
        data: {},
      }));
      TABLE.mutate(rowsToInsert, error => {
        assert.ifError(error);
        assert.deepEqual(entryKeyMutates, [
          ['foo', 'bar', 'baz'],
          ['bar', 'baz'],
          ['bar'],
          ['bar'],
        ]);
        done();
      });
    });

    it('considers network errors towards the retry count', (done) => {
      responses = [
        [
          ['response', {code: 200}],
          ['data', entryResponses([1, 1, 0])],
          ['end'],
        ], [
          ['response', {code: 429}],
          ['end'],
        ], [
          ['response', {code: 200}],
          ['data', entryResponses([1, 0])],
          ['end'],
        ], [
          ['response', {code: 200}],
          ['data', entryResponses([1])],
          ['end'],
        ]
      ];
      const keys = ['foo', 'bar', 'baz'];
      const rowsToInsert = keys.map(key => ({
        method: 'insert',
        key,
        data: {},
      }));
      TABLE.mutate(rowsToInsert, error => {
        assert(error.name, 'table insert should return an error');
        assert.equal(error.name, 'PartialFailureError');
        assert.deepEqual(entryKeyMutates, [
          ['foo', 'bar', 'baz'],
          ['foo', 'bar'],
          ['foo', 'bar'],
          ['foo'],
        ]);
        done();
      });
    });

    it('has a `PartialFailureError` error when an entry fails after the retries', (done) => {
      responses = [
        [
          ['response', {code: 200}],
          ['data', entryResponses([0, 1, 0])],
          ['end'],
        ], [
          ['response', {code: 200}],
          ['data', entryResponses([1])],
          ['end'],
        ], [
          ['response', {code: 200}],
          ['data', entryResponses([1])],
          ['end'],
        ], [
          ['response', {code: 200}],
          ['data', entryResponses([1])],
          ['end'],
        ]
      ];
      const keys = ['foo', 'bar', 'baz'];
      const rowsToInsert = keys.map(key => ({
        method: 'insert',
        key,
        data: {},
      }));
      TABLE.mutate(rowsToInsert, error => {
        assert(error.name, 'table insert should return an error');
        assert.equal(error.name, 'PartialFailureError');
        assert.deepEqual(entryKeyMutates, [
          ['foo', 'bar', 'baz'],
          ['bar'],
          ['bar'],
          ['bar'],
        ]);
        done();
      });
    });

  });
});