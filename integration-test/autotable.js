'use strict';

const Bigtable = require('../');

const tests = require('./mutate-rows-acceptance-test.json').tests;

const assert = require('assert');
const grpc = require('grpc');
const sinon = require('sinon');
const through = require('through2');

function chainEmits(emitter, response) {
  let emits = [['response', { code: response.code }]];
  if (response.entry_codes) {
    emits.push(['data', entryResponses(response.entry_codes)]);
  }
  emits.push(['end'])
  let index = 0;
  setImmediate(next);

  function next() {
    if (index < emits.length) {
      const emit = emits[index];
      index++;
      const name = emit[0];
      const data = emit[1];
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
    let entryKeyMutatesInvoked;
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
      entryKeyMutatesInvoked = [];
      mutateCallTimes = [];
      responses = null;
      stub = sinon.stub(bigtableService, 'mutateRows').callsFake((grpcOpts) => {
        entryKeyMutatesInvoked.push(grpcOpts.entries.map(entry => entry.rowKey.asciiSlice()));
        mutateCallTimes.push(new Date().getTime());
        const emitter = through.obj();
        chainEmits(emitter, responses.shift());
        return emitter;
      });
    });
    afterEach(() => {
      clock.uninstall();
      stub.restore();
    });

    tests.forEach(test => {
      it(test.name, (done) => {
        responses = test.responses;
        TABLE.maxRetries = test.max_retries;
        TABLE.mutate(test.mutations_request, (error) => {
          assert.deepEqual(entryKeyMutatesInvoked, test.mutations_batches_invoked);
          assert.strictEqual(mutateCallTimes[0], 0);
          getDeltas(mutateCallTimes).forEach((delta, index) => {
            if (index === 0) {
              assert.strictEqual(index, 0, 'First request should happen Immediately');
              return;
            }
            const minBackoff = 1000 * Math.pow(2, index);
            const maxBackoff = minBackoff + 1000;
            const message = `Backoff for retry ${index} should be between ` +
              `${minBackoff} and ${maxBackoff}, was ${delta}`;
            assert(delta > minBackoff, message);
            assert(delta < maxBackoff, message);
          });
          if (test.errors) {
            const expectedIndices = test.errors.map(error => {
              return error.index_in_mutations_request;
            });
            const actualIndices = error.errors.map(error => {
              return test.mutations_request.indexOf(error.entry);
            });
            assert.deepEqual(expectedIndices, actualIndices)
          } else {
            assert.ifError(error);
          }
          done();
        });
        clock.runAll();
      });
    });


  });
});