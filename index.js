/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

'use strict';

const async = require('async');
const _ = require('lodash');
const io = require('socket.io-client');
const deepEqual = require('deep-equal');
const debug = require('debug')('socketcuster');
const engineUtil = require('artillery/engine_util');
const EngineHttp = require('artillery/engine_http');
const template = engineUtil.template;

module.exports = class SocketCusterEngine {

  static markEndTime(ee, context, startedAt) {
    let endedAt = process.hrtime(startedAt);
    let delta = (endedAt[0] * 1e9) + endedAt[1];
    ee.emit('histogram', 'engine.socketcuster.response_time', delta / 1e6);
  }

  static isResponseRequired(spec) {
    return (spec.emit && spec.emit.response && spec.emit.response.channel);
  }

  static isAcknowledgeRequired(spec) {
    return (spec.emit && spec.emit.acknowledge);
  }

  static processResponse(ee, data, response, context, callback) {
    // Do we have supplied data to validate?
    if (response.data && !deepEqual(data, response.data)) {
      debug('data is not valid:');
      debug(data);
      debug(response);

      let err = 'data is not valid';
      ee.emit('error', err);
      return callback(err, context);
    }

    // If no capture or match specified, then we consider it a success at this point...
    if (!response.capture && !response.match) {
      return callback(null, context);
    }

    // Construct the (HTTP) response...
    let fauxResponse = { body: JSON.stringify(data) };

    // Handle the capture or match clauses...
    engineUtil.captureOrMatch(response, fauxResponse, context, (err, result) => {
      // Were we unable to invoke captureOrMatch?
      if (err) {
        debug(data);
        ee.emit('error', err);
        return callback(err, context);
      }

      if (result !== null) {
        // Do we have any failed matches?
        let failedMatches = _.filter(result.matches, (v, k) => {
          return !v.success;
        });

        // How to handle failed matches?
        if (failedMatches.length > 0) {
          debug(failedMatches);
          // TODO: Should log the details of the match somewhere
          ee.emit('error', 'Failed match');
          return callback(new Error('Failed match'), context);
        }
        else {
          // Emit match events...
          // _.each(result.matches, (v, k) => {
          //   ee.emit('match', v.success, {
          //     expected: v.expected,
          //     got: v.got,
          //     expression: v.expression
          //   });
          // });

          // Populate the context with captured values
          _.each(result.captures, (v, k) => {
            context.vars[k] = v.value;
          });
        }

        // Replace the base object context
        // Question: Should this be JSON object or String?
        context.vars.$ = fauxResponse.body;

        // Increment the success count...
        context._successCount++;

        return callback(null, context);
      }
    });
  }

  constructor(script) {
    this.config = script.config;

    this.socketcusterOpts = this.config.socketcuster || {};
    this.httpDelegate = new EngineHttp(script);
  }

  createScenario(scenarioSpec, ee) {
    // Adds scenario overridden configuration into the static config
    this.socketcusterOpts = { ...this.socketcusterOpts, ...scenarioSpec.socketcuster }

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
        if (!rs.emit && !rs.loop) {
          return this.httpDelegate.step(rs, ee);
        }
        return this.step(rs, ee);
      });

      return engineUtil.createLoopWithCount(
        requestSpec.count || -1,
        steps,
        {
          loopValue: requestSpec.loopValue,
          loopElement: requestSpec.loopElement || '$loopElement',
          overValues: requestSpec.over,
          whileTrue: this.config.processor ? this.config.processor[requestSpec.whileTrue] : undefined
        }
      );
    }

    let f = (context, callback) => {
      // Only process emit requests; delegate the rest to the HTTP engine (or think utility)
      if (requestSpec.think) {
        return engineUtil.createThink(requestSpec, _.get(this.config, 'defaults.think', {}));
      }
      if (!requestSpec.emit) {
        let delegateFunc = this.httpDelegate.step(requestSpec, ee);
        return delegateFunc(context, callback);
      }
      ee.emit('counter', 'engine.socketcuster.emit', 1);
      ee.emit('rate', 'engine.socketcuster.emit_rate');
      let startedAt = process.hrtime();
      let socketcuster = context.sockets[requestSpec.emit.namespace] || null;

      if (!(requestSpec.emit && requestSpec.emit.channel && socketcuster)) {
        debug('invalid arguments');
        ee.emit('error', 'invalid arguments');
        // TODO: Provide a more helpful message
        callback(new Error('socketcuster: invalid arguments'));
      }

      let outgoing = {
        channel: template(requestSpec.emit.channel, context),
        data: template(requestSpec.emit.data, context)
      };

      let endCallback = (err, context, needEmit) => {
        if (err) {
          debug(err);
        }

        if (this.constructor.isAcknowledgeRequired(requestSpec)) {
          let ackCallback = () => {
            let response = {
              data: template(requestSpec.emit.acknowledge.data, context),
              capture: template(requestSpec.emit.acknowledge.capture, context),
              match: template(requestSpec.emit.acknowledge.match, context)
            };
            // Make sure data, capture or match has a default json spec for parsing socketcuster responses
            _.each(response, (r) => {
              if (_.isPlainObject(r) && !('json' in r)) {
                r.json = '$.0'; // Default to the first callback argument
              }
            });
            // Acknowledge data can take up multiple arguments of the emit callback
            this.constructor.processResponse(ee, arguments, response, context, (err) => {
              if (!err) {
                this.constructor.markEndTime(ee, context, startedAt);
              }
              return callback(err, context);
            });
          }

          // Acknowledge required so add callback to emit
          if (needEmit) {
            socketcuster.emit(outgoing.channel, outgoing.data, ackCallback);
          } else {
            ackCallback();
          }
        } else {
          // No acknowledge data is expected, so emit without a listener
          if (needEmit) {
            socketcuster.emit(outgoing.channel, outgoing.data);
          }
          this.constructor.markEndTime(ee, context, startedAt);
          return callback(null, context);
        }
      }; // endCallback

      if (this.constructor.isResponseRequired(requestSpec)) {
        let response = {
          channel: template(requestSpec.emit.response.channel, context),
          data: template(requestSpec.emit.response.data, context),
          capture: template(requestSpec.emit.response.capture, context),
          match: template(requestSpec.emit.response.match, context)
        };
        // Listen for the socket.io response on the specified channel
        let done = false;
        socketcuster.on(response.channel, (data) => {
          done = true;
          this.constructor.processResponse(ee, data, response, context, (err) => {
            if (!err) {
              this.constructor.markEndTime(ee, context, startedAt);
            }
            // Stop listening on the response channel
            socketcuster.off(response.channel);
            return endCallback(err, context, false);
          });
        });
        // Send the data on the specified socket.io channel
        socketcuster.emit(outgoing.channel, outgoing.data);
        // If we don't get a response within the timeout, fire an error
        let waitTime = this.config.timeout || 10;
        waitTime *= 1000;
        setTimeout(() => {
          if (!done) {
            let err = 'response timeout';
            ee.emit('error', err);
            return callback(err, context);
          }
        }, waitTime);
      } else {
        endCallback(null, context, true);
      }
    };

    const preStep = (context, callback) => {
      // Set default namespace in emit action
      requestSpec.emit.namespace = template(requestSpec.emit.namespace, context) || "";

      this.loadContextSocket(requestSpec.emit.namespace, context, (err, socket) => {
        if (err) {
          debug(err);
          ee.emit('error', err.message);
          return callback(err, context);
        }

        return f(context, callback);
      });
    }

    if (requestSpec.emit) {
      return preStep;
    } else {
      return f;
    }
  }

  loadContextSocket(namespace, context, cb) {
    context.sockets = context.sockets || {};

    if (!context.sockets[namespace]) {
      let target = this.config.target + namespace;
      let tls = this.config.tls || {};

      const socketcusterOpts = template(this.socketcusterOpts, context);
      let options = _.extend(
        {},
        socketcusterOpts, // templated
        tls
      );

      let socket = io(target, options);
      context.sockets[namespace] = socket;
      wildcardPatch(socket);

      socket.on('*', () => {
        context.__receivedMessageCount++;
      });

      socket.once('connect', () => {
        cb(null, socket);
      });
      socket.once('connect_error', (err) => {
        cb(err, null);
      });
    } else {
      return cb(null, context.sockets[namespace]);
    }
  }

  closeContextSockets(context) {
    // if(context.socketcuster) {
    //   context.socketcuster.disconnect();
    // }
    if (context.sockets && Object.keys(context.sockets).length > 0) {
      var namespaces = Object.keys(context.sockets);
      namespaces.forEach((namespace) => {
        context.sockets[namespace].disconnect();
      });
    }
  }

  compile(tasks, scenarioSpec, ee) {
    const zero = (callback, context) => {
      context.__receivedMessageCount = 0;
      ee.emit('started');
      this.loadContextSocket('', context, (err) => {
        if (err) {
          ee.emit('error', err);
          return callback(err, context);
        }

        return callback(null, context);
      });
    }

    return (initialContext, callback) => {
      initialContext = this.httpDelegate.setInitialContext(initialContext);

      initialContext._pendingRequests = _.size(
        _.reject(scenarioSpec, (rs) => {
          return (typeof rs.think === 'number');
        }));

      let steps = _.flatten([
        (cb) => {
          return zero(cb, initialContext);
        },
        tasks
      ]);

      async.waterfall(
        steps,
        (err, context) => {
          if (err) {
            debug(err);
          }
          if (context) {
            this.closeContextSockets(context);
          }
          return callback(err, context);
        });
    };
  }
}