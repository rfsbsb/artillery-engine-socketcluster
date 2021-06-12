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
  }

  createScenario(scenarioSpec, ee) {
    let tasks = _.map(scenarioSpec.flow, (rs) => {
      if (rs.think) {
        return engineUtil.createThink(rs, _.get(this.config, 'defaults.think', {}));
      }

      return this.step(rs, ee);
    });

    return this.compile(tasks, scenarioSpec.flow, ee);
  }

  step(requestSpec, ee) {
    if (requestSpec.loop) {
      let steps = _.map(requestSpec.loop, (rs) => {
        return this.step(rs, ee);
      });

      return engineUtil.createLoopWithCount(
        requestSpec.count || -1,
        steps,
        {
          loopValue: requestSpec.loopValue || '$loopCount',
          overValues: requestSpec.over,
          whileTrue: this.config.processor ?
            this.config.processor[requestSpec.whileTrue] : undefined
        });
    }

    if (requestSpec.think) {
      return engineUtil.createThink(requestSpec, _.get(this.config, 'defaults.think', {}));
    }

    if (requestSpec.function) {
      return (context, callback) => {
        let processFunc = this.config.processor[requestSpec.function];
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

    const method = Object.keys(requestSpec)[0];
    const fn = `sc_${method}`;
    if (fn in this && _.isFunction(this[fn])) {
      return this[fn].bind(this, requestSpec[method]);
    }

    return (context, callback) => {
      debug('SC: no matching function found for step', JSON.stringify(requestSpec));
      return callback(null, context);
    }
  }

  sc_listener(params, context, callback) {
    return callback(null, context);
  }
  
  sc_closeListener(params, context, callback) {
    return callback(null, context);
  }

  sc_closeAllListeners(params, context, callback) {
    return callback(null, context);
  }

  sc_killListener(params, context, callback) {
    return callback(null, context);
  }

  sc_killAllListeners(params, context, callback) {
    return callback(null, context);
  }

  sc_receiver(params, context, callback) {
    return callback(null, context);
  }

  sc_closeReceiver(params, context, callback) {
    return callback(null, context);
  }

  sc_closeAllReceivers(params, context, callback) {
    return callback(null, context);
  }

  sc_killReceiver(params, context, callback) {
    return callback(null, context);
  }

  sc_killAllReceivers(params, context, callback) {
    return callback(null, context);
  }

  sc_procedure(params, context, callback) {
    return callback(null, context);
  }

  sc_closeProcedure(params, context, callback) {
    return callback(null, context);
  }

  sc_closeAllProcedures(params, context, callback) {
    return callback(null, context);
  }

  sc_killProcedure(params, context, callback) {
    return callback(null, context);
  }

  sc_killAllProcedures(params, context, callback) {
    return callback(null, context);
  }

  sc_transmit(params, context, callback) {
    return callback(null, context);
  }

  sc_invoke(params, context, callback) {
    return callback(null, context);
  }

  sc_send(params, context, callback) {
    const ee = context.ee;

    ee.emit('counter', 'engine.socketcluster.messages_sent', 1);
    ee.emit('rate', 'engine.socketcluster.send_rate');

    let startedAt = process.hrtime();

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
  }

  sc_authenticate(params, context, callback) {
    return callback(null, context);
  }

  sc_deauthenticate(params, context, callback) {
    return callback(null, context);
  }

  sc_transmitPublish(params, context, callback) {
    return callback(null, context);
  }

  sc_invokePublish(params, context, callback) {
    return callback(null, context);
  }

  sc_subscribe(params, context, callback) {
    const ee = context.ee;

    ee.emit('counter', 'engine.socketcluster.subscription', 1);
    ee.emit('rate', 'engine.socketcluster.subscription_rate');

    let startedAt = process.hrtime();

    debug('SC subscribe: %s', JSON.stringify(params));

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

    const channel = context.socket.subscribe(params.channel, params.options || {});

    // only process response if we're capturing
    if (params.capture) {
      // Listen for messages
      (async () => {
        for await (let data of channel) {
          messageHandler(data);
        }
      })();
    }
  }

  sc_unsubscribe(params, context, callback) {
    return callback(null, context);
  }

  sc_closeChannel(params, context, callback) {
    return callback(null, context);
  }

  sc_channelCloseOutput(params, context, callback) {
    return callback(null, context);
  }

  sc_channelCloseAllListeners(params, context, callback) {
    return callback(null, context);
  }

  sc_closeAllChannels(params, context, callback) {
    return callback(null, context);
  }

  sc_killChannel(params, context, callback) {
    return callback(null, context);
  }

  sc_channelKillOutput(params, context, callback) {
    return callback(null, context);
  }

  sc_channelKillAllListeners(params, context, callback) {
    return callback(null, context);
  }

  sc_killAllChannels(params, context, callback) {
    return callback(null, context);
  }

  sc_killAllChannelOutputs(params, context, callback) {
    return callback(null, context);
  }

  sc_killAllChannelListeners(params, context, callback) {
    return callback(null, context);
  }

  connect(context, callback) {
    const ee = context.ee;
    const config = this.config;    
    const tls = config.tls || {};
    const options = _.extend({
      hostname: this.config.target
    }, tls, config.socketcluster);

    ee.emit('started');

    context.socket = socketclusterClient.create(options);

    // Listen for connection
    (async () => {
      for await (let event of context.socket.listener('connect')) {
        context.socket.closeListener('connect');
        callback(null, context);
      }
    })();
    
    // Listen for connection error once
    (async () => {
      for await (let event of context.socket.listener('connectAbort')) {
        context.socket.closeListener('connectAbort');
        debug(event);
        ee.emit('error', `${event.code}:${event.reason}`);
        callback(event, {});
      }
    })();
  }

  disconnect(context) {
    if (context && context.socket) {
      context.socket.disconnect();
    }
  }

  compile(tasks, scenarioSpec, ee) {
    return (initialContext, callback) => {
      let steps = _.flatten([
        (callback) => {
          initialContext._successCount = 0;
          initialContext.ee = ee;
          callback(null, initialContext);
        },
        this.connect.bind(this),
        tasks
      ]);

      async.waterfall(
        steps,
        (err, context) => {
          if (err) {
            debug(err);
          }

          this.disconnect(context);

          return callback(err, context);
        });
    };
  }
}
