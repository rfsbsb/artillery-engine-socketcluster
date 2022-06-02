'use strict';

const async = require('async');
const _ = require('lodash');
const socketclusterClient = require('socketcluster-client');
const debug = require('debug')('socketcluster');
const engineUtil = require('artillery/core/lib/engine_util');
const template = engineUtil.template;

module.exports = class SocketCusterEngine {

  /**
   * A list of SC methods that do not return anything.
   *
   * Those methods cannot capture or match a result,
   * and use the common helper method invokeSCMethodWithoutCapture.
   *
   * @type {object}
   */
  static scMethodsWithoutCapture = {
    'closeListener': { 'eventName': null },
    'closeAllListeners': {},
    'killListener': {},
    'killAllListeners': {},
    'closeReceiver': { 'receiverName': null },
    'sc_closeAllReceivers': {},
    'killReceiver': { 'receiverName': null },
    'killAllReceivers': {},
    'closeProcedure': { 'procedureName': null },
    'closeAllProcedures': {},
    'killProcedure': { 'procedureName': null },
    'killAllProcedures': {},
    'transmit': { 'receiverName': null, 'data': {} },
    'send': { 'data': null, 'options': {} },
    'deauthenticate': {},
    'transmitPublish': { 'channelName': null, 'data': {} },
    'unsubscribe': { 'channel': null },
    'closeChannel': { 'channel': null },
    'channelCloseOutput': { 'channel': null },
    'channelCloseAllListeners': { 'channel': null },
    'closeAllChannels': {},
    'killChannel': { 'channelName': null },
    'channelKillOutput': { 'channelName': null },
    'channelKillAllListeners': { 'channelName': null },
    'killAllChannels': {},
    'killAllChannelOutputs': {},
    'killAllChannelListeners': {},
  };

  /**
   * Create a SocketCusterEngine instance
   *
   * @param {object} script The test script
   */
  constructor(script) {
    this.config = script.config;
    this._users = 0;
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

    let method = null;
    Object.keys(requestSpec).some((key) => {
      // Check if the sc method is implemented
      const fn = `sc_${key}`;
      if (fn in this && _.isFunction(this[fn])) {
        method = (context, callback) => {
          this[fn](
            template(requestSpec[key], context),
            context,
            callback
          );
        };
        return true;
      }

      // if no implementatio found, check if the method is listed
      // in scMethodsWithoutCapture
      if (key in this.constructor.scMethodsWithoutCapture) {
        method = (context, callback) => {
          this.invokeSCMethodWithoutCapture(
            key,
            this.constructor.scMethodsWithoutCapture[key],
            template(requestSpec[key], context),
            context,
            callback
          );
        };
        return true;
      }
    });
    if (method) {
      return method;
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
    const auto_create = !('autoCreate' in config.socketcluster) || config.socketcluster.autoCreate;

    ee.emit('started');

    if (auto_create) {
      this.sc_create({}, context, callback);
    }
    else {
      callback(null, context);
    }
  }

  /**
   * Compile the list of tasks
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
    const ee = context.ee;

    debug('SC capture or match: %s', data);

    if (!data) {
      return callback(new Error('Empty response from SC server'), context);
    }

    let fauxResponse;
    try {
      fauxResponse = { body: JSON.parse(data) };
    }
    catch (error) {
      fauxResponse = { body: data }
    }

    engineUtil.captureOrMatch(
      params,
      fauxResponse,
      context,
      (error, result) => {
        if (error) {
          ee.emit('error', error.message || error.code);
          return callback(error, context);
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
  invokeSCMethodWithoutCapture(method, args, params, context, callback) {
    const ee = context.ee;

    ee.emit('counter', `engine.socketcluster.${method}`, 1);
    ee.emit('rate', `engine.socketcluster.${method}_rate`);

    debug(`SC ${method}: %s`, JSON.stringify(params));

    const args_values = Object.entries(args).map(([key, value]) => {
      return key in params ? params[key] : value;
    });

    context.socket[method](...args_values);

    return callback(null, context);
  }

  /**
   * Implementation of the create task
   *
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_create(params, context, callback) {
    const config = this.config;
    const tls = config.tls || {};
    const options = _.extend({
      hostname: config.target
    }, tls, config.socketcluster, params);

    context.socket = socketclusterClient.create(options);

    if (!('autoConnect' in options) || options.autoConnect) {
      return this.sc_connect({}, context, callback);
    }

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
    const ee = context.ee;

    context.socket.connect();

    // Listen for connection
    (async () => {
      try {
        await context.socket.listener('connect').once();
        context.socket.killListener('connectAbort');
        callback(null, context);

        this._users++;
        context.ee.emit('histogram', 'engine.socketcluster.users', this._users);
      }
      catch (error) {
        ee.emit('error', `${error.name}: ${error.message}`);
        callback(error, null);
      }
    })();

    // Listen for connection error once
    (async () => {
      const error = await context.socket.listener('connectAbort').once();
      context.socket.killListener('connect');

      ee.emit('error', `${error.code}: ${error.reason}`);
      callback(error, null);
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
   * Implementation of the receiver task
   *
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_receiver(params, context, callback) {
    const receiver = context.socket.receiver(params.receiverName);

    if (params.capture || params.match) {
      return this.captureOrMatch(receiver, params, context, callback);
    }

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
    const procedure = context.socket.procedure(params.procedureName);

    if (params.capture || params.match) {
      return this.captureOrMatch(procedure, params, context, callback);
    }

    return callback(null, context);
  }

  /**
   * Implementation of the invoke task
   *
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_invoke(params, context, callback) {
    const startedAt = process.hrtime();
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
        .catch((error) => {
          ee.emit('error', `${error.name}: ${error.message}`);
          callback(error, context);
        })
        .finally(() => {
          const endedAt = process.hrtime(startedAt);
          const delta = (endedAt[0] * 1e9) + endedAt[1];
          ee.emit('histogram', 'engine.socketcluster.response_time', delta / 1e6);
        });
    })();
  }

  /**
   * Implementation of the authenticate task
   *
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_authenticate(params, context, callback) {
    const startedAt = process.hrtime();
    const ee = context.ee;

    ee.emit('counter', 'engine.socketcluster.authenticate', 1);
    ee.emit('rate', 'engine.socketcluster.authenticate_rate');

    debug('SC authenticate: %s', JSON.stringify(params));

    context.socket.authenticate(params.token)
      .then(() => {
        callback(null, context);
      })
      .catch((error) => {
        ee.emit('error', `${error.name}: ${error.message}`);
        callback(error, context);
      })
      .finally(() => {
        const endedAt = process.hrtime(startedAt);
        const delta = (endedAt[0] * 1e9) + endedAt[1];
        ee.emit('histogram', 'engine.socketcluster.response_time', delta / 1e6);
      });
  }

  /**
   * Implementation of the invokePublish task
   *
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_invokePublish(params, context, callback) {
    const startedAt = process.hrtime();
    const ee = context.ee;

    ee.emit('counter', 'engine.socketcluster.invokePublish', 1);
    ee.emit('rate', 'engine.socketcluster.invokePublish_rate');

    debug('SC invokePublish: %s', JSON.stringify(params));

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
      .catch((error) => {
        ee.emit('error', `${error.name}: ${error.message}`);
        callback(error, context);
      })
      .finally(() => {
        const endedAt = process.hrtime(startedAt);
        const delta = (endedAt[0] * 1e9) + endedAt[1];
        ee.emit('histogram', 'engine.socketcluster.response_time', delta / 1e6);
      });
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
        const data = await channel.once();
        this.captureOrMatch(data, params, context, callback);
      })();
    }
    else {
      return callback(null, context);
    }
  }

  /**
   * Implementation of the subscriptions task
   *
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_subscriptions() {
    const subscriptions = context.socket.subscriptions(params.includePending ?? false);

    if (params.capture || params.match) {
      return this.captureOrMatch(subscriptions, params, context, callback);
    }

    return callback(null, context);
  }

  /**
   * Implementation of the isSubscribed task
   *
   * @param {object} params The tasks parameters
   * @param {object} context The tasks context
   * @param {function} callback The tasks callback
   */
  sc_isSubscribed() {
    const subscribed = context.socket.isSubscribed(
      params.channelName,
      params.includePending ?? false
    );

    if (params.capture || params.match) {
      return this.captureOrMatch(subscribed, params, context, callback);
    }

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

      this._users--;
      context.ee.emit('histogram', 'engine.socketcluster.users', this._users);
    }
  }
}
