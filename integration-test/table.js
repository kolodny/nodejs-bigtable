'use strict';

const Bigtable = require('../');

const assert = require('assert');
const grpc = require('grpc');
const sinon = require('sinon');
const through = require('through2');

function chainEmits(emitter, responses) {
  let index = 0;
  setImmediate(next);

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

function getDeltas(array) {
  return array.reduce((acc, item, index) => {
    return index ? acc.concat(item - array[index - 1]) : [item];
  }, [])
}

describe.skip('Bigtable/Table', () => {
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
    let mutateCallTimes;
    let responses;
    let stub;

    beforeEach(() => {

      clock = sinon.useFakeTimers({ 
        toFake: [
          'setTimeout',
          'clearTimeout',
          'setImmediate',
          'clearImmediate',
          'setInterval',
          'clearInterval',
          'Date',
          'nextTick',
        ],
      });
      entryKeyMutates = [];
      mutateCallTimes = [];
      responses = null;
      stub = sinon.stub(bigtableService, 'mutateRows').callsFake((grpcOpts) => {
        entryKeyMutates.push(grpcOpts.entries.map(entry => entry.rowKey.asciiSlice()));
        mutateCallTimes.push(new Date().getTime());
        const response = through.obj();
        chainEmits(response, responses.shift());
        return response;
      });
    });
    afterEach(() => {
      console.log(getDeltas(mutateCallTimes))
      clock.uninstall();
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
      clock.runAll();
    });

    it('retries the failed mutations', (done) => {
      responses = [
        [
          ['response', {code: 200}],
          ['data', entryResponses([0, 4, 4])],
          ['end'],
        ], [
          ['response', {code: 200}],
          ['data', entryResponses([4, 0])],
          ['end'],
        ], [
          ['response', {code: 200}],
          ['data', entryResponses([4])],
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
      clock.runAll();
    });

    it('considers network errors towards the retry count', (done) => {
      responses = [
        [
          ['response', {code: 200}],
          ['data', entryResponses([4, 4, 0])],
          ['end'],
        ], [
          ['response', {code: 429}],
          ['end'],
        ], [
          ['response', {code: 200}],
          ['data', entryResponses([4, 0])],
          ['end'],
        ], [
          ['response', {code: 200}],
          ['data', entryResponses([4])],
          ['end'],
        ], [
          ['response', {code: 200}],
          ['data', entryResponses([4])],
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
      clock.runAll();
    });

    it('has a `PartialFailureError` error when an entry fails after the retries', (done) => {
      debugger;
      responses = [
        [
          ['response', {code: 200}],
          ['data', entryResponses([0, 4, 0])],
          ['end'],
        ], [
          ['response', {code: 200}],
          ['data', entryResponses([4])],
          ['end'],
        ], [
          ['response', {code: 200}],
          ['data', entryResponses([4])],
          ['end'],
        ], [
          ['response', {code: 200}],
          ['data', entryResponses([4])],
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
      clock.runAll();
    });

  });
});