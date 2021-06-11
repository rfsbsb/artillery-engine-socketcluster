/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

'use strict';

const async = require('async');
const _ = require('lodash');
const socketclusterClient = require('socketcluster-client');
const debug = require('debug')('socketcluster');
const engineUtil = require('artillery/core/lib/engine_util');
const template = engineUtil.template;

module.exports = class SocketCusterEngine {

  constructor(script) {
    this.config = script.config;

    this.socketcusterOpts = this.config.socketcluster || {};
  }

  createScenario(scenarioSpec, ee) {
    var self = this;
    let tasks = _.map(scenarioSpec.flow, (rs) => {
      if (rs.think) {
        return engineUtil.createThink(rs, _.get(self.config, 'defaults.think', {}));
      }

      return self.step(rs, ee);
    });

    return self.compile(tasks, scenarioSpec.flow, ee);
  }

  step(requestSpec, ee) {
    let self = this;

    if (requestSpec.loop) {
      let steps = _.map(requestSpec.loop, (rs) => {
        return self.step(rs, ee);
      });

      return engineUtil.createLoopWithCount(
        requestSpec.count || -1,
        steps,
        {
          loopValue: requestSpec.loopValue || '$loopCount',
          overValues: requestSpec.over,
          whileTrue: self.config.processor ?
            self.config.processor[requestSpec.whileTrue] : undefined
        });
    }

    if (requestSpec.think) {
      return engineUtil.createThink(requestSpec, _.get(self.config, 'defaults.think', {}));
    }

    if (requestSpec.function) {
      return (context, callback) => {
        let processFunc = self.config.processor[requestSpec.function];
        if (processFunc) {
          processFunc(context, ee, () => {
            return callback(null, context);
          });
        }
      }
    }

    if (requestSpec.log) {
      return (context, callback) => {
        console.log(template(requestSpec.log, context));
        return process.nextTick(() => { callback(null, context); });
      };
    }

    let f = (context, callback) => {
      ee.emit('counter', 'engine.socketcluster.messages_sent', 1);
      ee.emit('rate', 'engine.socketcluster.send_rate')
      let startedAt = process.hrtime();
      let params = requestSpec.send;

      // Close message listener to stop steps interfering with each other
      if (context.socket) {
        context.socket.closeListener('message');
      }

      debug('SC send: %s', JSON.stringify(params));

      const messageHandler = (event) => {
        const { data } = event;

        debug('SC receive: %s', data);

        if (!data) {
          return callback(new Error('Empty response from SC server'), context)
        }

        let fauxResponse;
        try {
          fauxResponse = { body: JSON.parse(data) };
        } catch (err) {
          fauxResponse = { body: event.data }
        }

        engineUtil.captureOrMatch(
          params,
          fauxResponse,
          context,
          (err, result) => {
            if (err) {
              ee.emit('error', err.message || err.code);
              return callback(err, context);
            }

            const { captures = {}, matches = {} } = result

            debug('captures and matches:');
            debug(matches);
            debug(captures);

            // match and capture are strict by default:
            const haveFailedMatches = _.some(result.matches, (v, k) => {
              return !v.success && v.strict !== false;
            });

            const haveFailedCaptures = _.some(result.captures, (v, k) => {
              return v.failed;
            });

            if (haveFailedMatches || haveFailedCaptures) {
              // TODO: Emit the details of each failed capture/match
              return callback(new Error('Failed capture or match'), context);
            }

            _.each(result.matches, (v, k) => {
              ee.emit('match', v.success, {
                expected: v.expected,
                got: v.got,
                expression: v.expression,
                strict: v.strict
              });
            });

            _.each(result.captures, (v, k) => {
              _.set(context.vars, k, v.value);
            });

            return callback(null, context);
          }
        )
      }

      // only process response if we're capturing
      if (params.capture) {
        // Listen for messages
        (async () => {
          for await (let event of context.socket.listener('message')) {
            messageHandler(event);
          }
        })();
      }

      context.socket.send(params.data, params.options || {});
    };

    return f;
  }

  compile(tasks, scenarioSpec, ee) {
    let config = this.config;

    return (initialContext, callback) => {
      const zero = (callback) => {
        let tls = config.tls || {};
        let options = _.extend({
          hostname: this.config.target
        }, tls, config.socketcluster);

        ee.emit('started');

        let socket = socketclusterClient.create(options);
  
        // Listen for connection
        (async () => {
          for await (let event of socket.listener('connect')) {
            socket.closeListener('connect');
            initialContext.socket = socket;
            callback(null, initialContext);
          }
        })();
        
        // Listen for connection error once
        (async () => {
          for await (let event of socket.listener('connectAbort')) {
            socket.closeListener('connectAbort');
            debug(event);
            ee.emit('error', `${event.code}:${event.reason}`);
            callback(event, {});
          }
        })();
      }

      initialContext._successCount = 0;

      let steps = _.flatten([
        zero,
        tasks
      ]);

      async.waterfall(
        steps,
        (err, context) => {
          if (err) {
            debug(err);
          }

          if (context && context.socket) {
            context.socket.disconnect();
          }

          return callback(err, context);
        });
    };
  }
}
