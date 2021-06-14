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

      return this.createStep(rs, ee);
    });

    return this.compile(tasks, scenarioSpec.flow, ee);
  }

  createStep(requestSpec, ee) {
    if (requestSpec.loop) {
      let steps = _.map(requestSpec.loop, (rs) => {
        return this.createStep(rs, ee);
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

    const method = Object.keys(requestSpec).find((key) => {
      const fn = `sc_${key}`;
      return (fn in this && _.isFunction(this[fn]));
    });
    if (method) {
      return this[`sc_${method}`].bind(this, requestSpec[method]);
    }

    return (context, callback) => {
      debug('SC: no matching function found for step', JSON.stringify(requestSpec));
      return callback(null, context);
    }
  }

  createClient(context, callback) {
    const ee = context.ee;
    const config = this.config;
    const tls = config.tls || {};
    const options = _.extend({
      hostname: config.target
    }, tls, config.socketcluster);
    const auto_connect = !('autoConnect' in options) || options.autoConnect;

    ee.emit('started');

    options.autoConnect = false;
    context.socket = socketclusterClient.create(options);

    if (auto_connect) {
      this.sc_connect({}, context, callback);
    }
    else {
      callback(null, context);
    }
  }

  compile(tasks, scenarioSpec, ee) {
    return (initialContext, callback) => {
      initialContext._successCount = 0;
      initialContext.ee = ee;

      let steps = _.flatten([
        this.createClient.bind(this, initialContext),
        tasks
      ]);

      async.waterfall(
        steps,
        (err, context) => {
          if (err) {
            debug(err);
          }

          this.sc_disconnect(context);

          return callback(err, context);
        });
    };
  }

  capture(data, params, context, callback) {
    debug('SC capture: %s', data);

    if (!data) {
      return callback(new Error('Empty response from SC server'), context);
    }

    let fauxResponse;
    try {
      fauxResponse = { body: JSON.parse(data) };
    } catch (err) {
      fauxResponse = { body: data }
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

  invokeSCMethodWithoutCapture(method, params, context, callback, ...args) {
    const ee = context.ee;

    ee.emit('counter', `engine.socketcluster.${method}`, 1);
    ee.emit('rate', `engine.socketcluster.${method}_rate`);

    debug(`SC ${method}: %s`, JSON.stringify(params));

    context.socket[method](...args);

    return callback(null, context);
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
    return this.invokeSCMethodWithoutCapture(
      'transmit',
      params, context, callback,
      params.receiverName, params.data
    );
  }

  sc_invoke(params, context, callback) {
    const ee = context.ee;

    ee.emit('counter', 'engine.socketcluster.invoke', 1);
    ee.emit('rate', 'engine.socketcluster.invoke_rate');

    debug('SC invoke: %s', JSON.stringify(params));

    (async () => {
      context.socket.invoke(params.procedureName, params.data)
        .then((result) => {
          // only process response if we're capturing
          if (params.capture) {
            this.capture(result, params, context, callback);
          }
          else {
            callback(null, context);
          }
        })
        .catch((err) => {
          callback(err, context);
        });
    })();
  }

  sc_send(params, context, callback) {
    return this.invokeSCMethodWithoutCapture(
      'send',
      params, context, callback,
      params.data, params.options || {}
    );
  }

  sc_authenticate(params, context, callback) {
    const ee = context.ee;

    ee.emit('counter', 'engine.socketcluster.authenticate', 1);
    ee.emit('rate', 'engine.socketcluster.authenticate_rate');

    debug('SC authenticate: %s', JSON.stringify(params));

    context.socket.authenticate(params.token)
      .then(() => {
        callback(null, context);
      })
      .catch((err) => {
        callback(err, context);
      });
  }

  sc_deauthenticate(params, context, callback) {
    return this.invokeSCMethodWithoutCapture(
      'deauthenticate',
      params, context, callback
    );
  }

  sc_transmitPublish(params, context, callback) {
    const ee = context.ee;

    ee.emit('counter', 'engine.socketcluster.transmitPublish', 1);
    ee.emit('rate', 'engine.socketcluster.transmitPublish_rate');

    debug('SC transmitPublish: %s', JSON.stringify(params));

    context.socket.transmitPublish(params.channelName, params.data);

    return callback(null, context);
  }

  sc_invokePublish(params, context, callback) {
    const ee = context.ee;

    ee.emit('counter', 'engine.socketcluster.invokePublish', 1);
    ee.emit('rate', 'engine.socketcluster.invokePublish_rate');

    debug('SC invokePublish: %s', JSON.stringify(params));

    (async () => {
      context.socket.invokePublish(params.channelName, params.data)
        .then((result) => {
          // only process response if we're capturing
          if (params.capture) {
            this.capture(result, params, context, callback);
          }
          else {
            callback(null, context);
          }
        })
        .catch((err) => {
          callback(err, context);
        });
    })();
  }

  sc_subscribe(params, context, callback) {
    const ee = context.ee;

    ee.emit('counter', 'engine.socketcluster.subscribe', 1);
    ee.emit('rate', 'engine.socketcluster.subscribe_rate');

    debug('SC subscribe: %s', JSON.stringify(params));

    const channel = context.socket.subscribe(params.channel, params.options || {});

    // only process response if we're capturing
    if (params.capture) {
      // Capture data
      (async () => {
        for await (let data of channel) {
          this.capture(data, params, context, callback);
        }
      })();
    }
    else {
      return callback(null, context);
    }
  }

  sc_unsubscribe(params, context, callback) {
    return this.invokeSCMethodWithoutCapture(
      'unsubscribe',
      params, context, callback,
      params.channel
    );
  }

  sc_closeChannel(params, context, callback) {
    return this.invokeSCMethodWithoutCapture(
      'closeChannel',
      params, context, callback,
      params.channel
    );
  }

  sc_channelCloseOutput(params, context, callback) {
    return this.invokeSCMethodWithoutCapture(
      'channelCloseOutput',
      params, context, callback,
      params.channel
    );
  }

  sc_channelCloseAllListeners(params, context, callback) {
    return this.invokeSCMethodWithoutCapture(
      'channelCloseAllListeners',
      params, context, callback,
      params.channel
    );
  }

  sc_closeAllChannels(params, context, callback) {
    return this.invokeSCMethodWithoutCapture(
      'closeAllChannels',
      params, context, callback
    );
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

  sc_connect(params, context, callback) {
    context.socket.connect();

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
        callback(event, null);
      }
    })();
  }

  sc_disconnect(context) {
    if (context && context.socket) {
      context.socket.disconnect();
    }
  }
}
