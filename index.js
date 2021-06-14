'use strict';

const async = require('async');
const _ = require('lodash');
const socketclusterClient = require('socketcluster-client');
const debug = require('debug')('socketcluster');
const engineUtil = require('artillery/core/lib/engine_util');
const template = engineUtil.template;

module.exports = class SocketCusterEngine {

  /**
   * Create a SocketCusterEngine instance
   * 
   * @param {object} script The test script
   */
  constructor(script) {
    this.config = script.config;
  }

  /**
   * Create a scenario
   * 
   * @param {object} scenarioSpec The scenario specification
   * @param {EventEmitter} ee The artillery event emitter
   * @returns {function} The compiled list of tasks
   */
  createScenario(scenarioSpec, ee) {
    let tasks = _.map(scenarioSpec.flow, (rs) => {
      if (rs.think) {
        return engineUtil.createThink(rs, _.get(this.config, 'defaults.think', {}));
      }

      return this.createStep(rs, ee);
    });

    return this.compile(tasks, scenarioSpec.flow, ee);
  }

  /**
   * Create a scenario step
   * 
   * @private
   * @param {object} requestSpec The step's specification
   * @param {EventEmitter} ee The artillery event emitter
   * @returns {function} The steps's function
   */
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

  /**
   * Create a virtual user
   * 
   * @param {object} context The context
   * @param {function} callback The callback to invoke on success or failure
   */
  createVirtualUser(context, callback) {
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

  /**
   * Compile the lsit of tasks
   * 
   * @param {Array} tasks The tasks to perform
   * @param {object} scenarioSpec The scenario specification
   * @param {EventEmitter} ee The artillery event emitter
   * @returns {function} A function that runs the tasks in sequence
   */
  compile(tasks, scenarioSpec, ee) {
    return (initialContext, callback) => {
      initialContext._successCount = 0;
      initialContext.ee = ee;

      let steps = _.flatten([
        this.createVirtualUser.bind(this, initialContext),
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

  /**
   * Capture or match a variable in a task's returned data
   * 
   * @private
   * @param {*} data The data from which to capture or match a variable
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   * @returns 
   */
   captureOrMatch(data, params, context, callback) {
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

  /**
   * Helper that invokes a sc method from a tasks that doesn't
   * need to capture or match variables in the returned value
   * 
   * @param {string} method The sc method's name
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   * @param  {...any} args Arguments to pass to the sc method
   * @returns 
   */
  invokeSCMethodWithoutCapture(method, params, context, callback, ...args) {
    const ee = context.ee;

    ee.emit('counter', `engine.socketcluster.${method}`, 1);
    ee.emit('rate', `engine.socketcluster.${method}_rate`);

    debug(`SC ${method}: %s`, JSON.stringify(params));

    context.socket[method](...args);

    return callback(null, context);
  }

  /**
   * Implementation of the connect task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_connect(params, context, callback) {
    context.socket.connect();

    // Listen for connection
    (async () => {
      await context.socket.listener('connect').once();
      callback(null, context);
    })();

    // Listen for connection error once
    (async () => {
      const event = await context.socket.listener('connectAbort').once();
      callback(event, null);
    })();
  }

  /**
   * Implementation of the listener task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_listener(params, context, callback) {
    const listener = context.socket.listener(params.eventName);

    if (params.capture || params.match) {
      (async () => {
        const event = await listener.once();
        this.captureOrMatch(event, params, context, callback);
      })();
    }
    else {
      return callback(null, context);
    }
  }

  /**
   * Implementation of the closeListener task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_closeListener(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the closeAllListeners task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_closeAllListeners(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the killListener task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_killListener(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the killAllListeners task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_killAllListeners(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the receiver task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_receiver(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the closeReceiver task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_closeReceiver(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the closeAllReceivers task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_closeAllReceivers(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the killReceiver task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_killReceiver(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the killAllReceivers task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_killAllReceivers(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the procedure task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_procedure(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the closeProcedure task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_closeProcedure(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the closeAllProcedures( task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_closeAllProcedures(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the killProcedure task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_killProcedure(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the killAllProcedures task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_killAllProcedures(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the transmit task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_transmit(params, context, callback) {
    return this.invokeSCMethodWithoutCapture(
      'transmit',
      params, context, callback,
      params.receiverName, params.data
    );
  }

  /**
   * Implementation of the invoke task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_invoke(params, context, callback) {
    const ee = context.ee;

    ee.emit('counter', 'engine.socketcluster.invoke', 1);
    ee.emit('rate', 'engine.socketcluster.invoke_rate');

    debug('SC invoke: %s', JSON.stringify(params));

    (async () => {
      context.socket.invoke(params.procedureName, params.data)
        .then((result) => {
          // only process response if we're capturing
          if (params.capture || params.match) {
            this.captureOrMatch(result, params, context, callback);
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

  /**
   * Implementation of the send task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_send(params, context, callback) {
    return this.invokeSCMethodWithoutCapture(
      'send',
      params, context, callback,
      params.data, params.options || {}
    );
  }

  /**
   * Implementation of the authenticate task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
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

  /**
   * Implementation of the deauthenticate task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_deauthenticate(params, context, callback) {
    return this.invokeSCMethodWithoutCapture(
      'deauthenticate',
      params, context, callback
    );
  }

  /**
   * Implementation of the transmitPublish task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_transmitPublish(params, context, callback) {
    return this.invokeSCMethodWithoutCapture(
      'transmitPublish',
      params, context, callback,
      params.channelName, params.data
    );
  }

  /**
   * Implementation of the invokePublish task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_invokePublish(params, context, callback) {
    const ee = context.ee;

    ee.emit('counter', 'engine.socketcluster.invokePublish', 1);
    ee.emit('rate', 'engine.socketcluster.invokePublish_rate');

    debug('SC invokePublish: %s', JSON.stringify(params));

    (async () => {
      context.socket.invokePublish(params.channelName, params.data)
        .then((result) => {
          // only process response if we're capturing
          if (params.capture || params.match) {
            this.captureOrMatch(result, params, context, callback);
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

  /**
   * Implementation of the subscribe task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_subscribe(params, context, callback) {
    const ee = context.ee;

    ee.emit('counter', 'engine.socketcluster.subscribe', 1);
    ee.emit('rate', 'engine.socketcluster.subscribe_rate');

    debug('SC subscribe: %s', JSON.stringify(params));

    const channel = context.socket.subscribe(params.channel, params.options || {});

    if (params.capture || params.match) {
      (async () => {
        const data = channel.once();
        this.captureOrMatch(data, params, context, callback);
      })();
    }
    else {
      return callback(null, context);
    }
  }

  /**
   * Implementation of the unsubscribe task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_unsubscribe(params, context, callback) {
    return this.invokeSCMethodWithoutCapture(
      'unsubscribe',
      params, context, callback,
      params.channel
    );
  }

  /**
   * Implementation of the closeChannel task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_closeChannel(params, context, callback) {
    return this.invokeSCMethodWithoutCapture(
      'closeChannel',
      params, context, callback,
      params.channel
    );
  }

  /**
   * Implementation of the channelCloseOutput task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_channelCloseOutput(params, context, callback) {
    return this.invokeSCMethodWithoutCapture(
      'channelCloseOutput',
      params, context, callback,
      params.channel
    );
  }

  /**
   * Implementation of the channelCloseAllListeners task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_channelCloseAllListeners(params, context, callback) {
    return this.invokeSCMethodWithoutCapture(
      'channelCloseAllListeners',
      params, context, callback,
      params.channel
    );
  }

  /**
   * Implementation of the closeAllChannels task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_closeAllChannels(params, context, callback) {
    return this.invokeSCMethodWithoutCapture(
      'closeAllChannels',
      params, context, callback
    );
  }

  /**
   * Implementation of the killChannel task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_killChannel(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the channelKillOutput task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_channelKillOutput(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the channelKillAllListeners task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_channelKillAllListeners(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the killAllChannels task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_killAllChannels(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the killAllChannelOutputs task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_killAllChannelOutputs(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the killAllChannelListeners task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_killAllChannelListeners(params, context, callback) {
    return callback(null, context);
  }

  /**
   * Implementation of the disconnect task
   * 
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_disconnect(context) {
    if (context && context.socket) {
      context.socket.disconnect();
    }
  }
}
