(function e(t,n,r){function s(o,u){if(!n[o]){if(!t[o]){var a=typeof require=="function"&&require;if(!u&&a)return a(o,!0);if(i)return i(o,!0);throw new Error("Cannot find module '"+o+"'")}var f=n[o]={exports:{}};t[o][0].call(f.exports,function(e){var n=t[o][1][e];return s(n?n:e)},f,f.exports,e,t,n,r)}return n[o].exports}var i=typeof require=="function"&&require;for(var o=0;o<r.length;o++)s(r[o]);return s})({1:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

/**
 * Media API for the Kurento Web SDK
 *
 * @module KwsMedia
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var EventEmitter = require('events').EventEmitter;
var extend       = require('extend');
var inherits     = require('inherits');
var url          = require('url');

var Promise = require('es6-promise').Promise;

var async = require('async');
var WebSocket = require('ws');

var RpcBuilder = require('kws-rpc-builder');
var JsonRPC    = RpcBuilder.packers.JsonRPC;

var checkType   = require('./checkType');
var checkParams = checkType.checkParams;

var utils = require('./utils');

var noop            = utils.noop;
var promiseCallback = utils.promiseCallback;


// Remote classes

var core      = require('./core');
var endpoints = require('./endpoints');
var filters   = require('./filters');
var hubs      = require('./hubs');

var Hub         = require('./core/Hub');
var MediaObject = require('./core/MediaObject');

var HubPort       = core.HubPort;
var MediaPipeline = core.MediaPipeline;

// Remote abstract classes
var abstracts =
{
  // Core
  Filter:       require('./core/Filter'),
  Hub:          Hub,
  MediaElement: require('./core/MediaElement'),
  MediaPad:     require('./core/MediaPad'),
  MediaSink:    require('./core/MediaSink'),
  MediaSource:  require('./core/MediaSource'),

  // Endpoints
  HttpEndpoint:    require('./endpoints/HttpEndpoint'),
  SdpEndpoint:     require('./endpoints/SdpEndpoint'),
  SessionEndpoint: require('./endpoints/SessionEndpoint'),
  UriEndpoint:     require('./endpoints/UriEndpoint')
};


/**
 * Creates a connection with the Kurento Media Server
 *
 * @class
 *
 * @param {external:String} uri - Address of the Kurento Media Server
 */
function KwsMedia(uri, options, onconnect, onerror)
{
  if(!(this instanceof KwsMedia))
    return new KwsMedia(uri, options, onconnect, onerror);

  var self = this;

  EventEmitter.call(this);


  // Fix optional parameters
  if(options instanceof Function)
  {
    onerror   = onconnect;
    onconnect = options;
    options   = undefined;
  };

  options = options || {};


  var objects = {};


  function onNotification(message)
  {
    var method = message.method;
    var params = message.params.value;

    var id = params.object;

    var object = objects[id];
    if(!object)
      return console.warn("Unknown object id '"+id+"'", message);

    switch(method)
    {
      case 'onEvent':
        object.emit(params.type, params.data);
      break;

//      case 'onError':
//        object.emit('error', params.error);
//      break;

      default:
        console.warn("Unknown message type '"+method+"'");
    };
  };


  //
  // JsonRPC
  //

  var ws;
  if(typeof uri == 'string')
  {
    var access_token = options.access_token;
    if(access_token != undefined)
    {
      uri = url.parse(uri, true);
      uri.query.access_token = access_token;
      uri = url.format(uri);

      delete options.access_token;
    };

    ws = new WebSocket(uri);
  }

  // URI is the WebSocket itself
  else
    ws = uri;


  var rpc = new RpcBuilder(JsonRPC, ws, function(request)
  {
    if(request instanceof RpcBuilder.RpcNotification)
    {
      // Message is an unexpected request, notify error
      if(request.duplicated != undefined)
        return console.warning('Unexpected request:', request);

      // Message is a notification, process it
      return onNotification(request);
    };

    // Invalid message, notify error
    console.error('Invalid request instance', request);
  });

  ws.addEventListener('close', function(event)
  {
    self.emit('disconnect', event);
  });


  // Promise interface ("thenable")

  this.then = function(onFulfilled, onRejected)
  {
    return new Promise(function(resolve, reject)
    {
      function success()
      {
        var result;

        if(onFulfilled)
          try
          {
            result = onFulfilled(self);
          }
          catch(exception)
          {
            return reject(exception);
          }

        resolve(result);
      };
      function failure()
      {
        var result;

        if(onRejected)
          try
          {
            result = onRejected(new Error('Connection error'));
          }
          catch(exception)
          {
            return reject(exception);
          }

        reject(result);
      };

      if(ws.readyState == ws.OPEN)
        success()
      else
        ws.addEventListener('open', success);

      if(ws.readyState > ws.OPEN)
        failure()
      else
        ws.addEventListener('error', failure);
    });
  };

  this.catch = function(onRejected)
  {
    return this.then(null, onRejected);
  };

  this.then(onconnect, onerror);


  this.close = ws.close.bind(ws);


  // Serialize objects using their id
  function serializeParams(params)
  {
    for(var key in params)
    {
      var param = params[key];
      if(param instanceof MediaObject)
        params[key] = param.id;
    };

    return params;
  };


  function createObject(constructor, id, params)
  {
    var mediaObject = new constructor(id, params);

    /**
     * Request to release the object on the server and remove it from cache
     */
    mediaObject.on('release', function()
    {
      delete objects[id];
    });

    /**
     * Request a generic functionality to be procesed by the server
     */
    mediaObject.on('_rpc', function(method, params, callback)
    {
      params.object = id;

      // Serialize objects using their id
      params.operationParams = serializeParams(params.operationParams);

      rpc.encode(method, params, function(error, result)
      {
        if(error) return callback(error);

        var operation = params.operation;

        if(operation == 'getConnectedSinks'
        || operation == 'getMediaSinks'
        || operation == 'getMediaSrcs')
        {
          var sessionId = result.sessionId;

          return self.getMediaobjectById(result.value, function(error, result)
          {
            var result =
            {
              sessionId: sessionId,
              value: result
            };

            callback(error, result);
          });
        };

        callback(null, result);
      });
    });

    if(mediaObject instanceof Hub
    || mediaObject instanceof MediaPipeline)
      mediaObject.on('_create', self.create.bind(self));

    objects[id] = mediaObject;

    return mediaObject;
  };

  function getConstructor(type)
  {
    // If element type is not registered or public, use generic MediaObject
    return core[type] || endpoints[type] || filters[type] || hubs[type]
        || abstracts[type] || MediaObject;
  };

  /**
   * Request to the server to create a new MediaElement
   */
  function createMediaObject(item, callback)
  {
    var type = item.type;

    var constructor = getConstructor(type);

    if(constructor.create)
    {
      item = constructor.create(item.params);

      // Apply inheritance
      var prototype = constructor.prototype;
      inherits(constructor, getConstructor(item.type));
      extend(constructor.prototype, prototype);
    };

    item.constructorParams = checkParams(item.params,
                                         constructor.constructorParams, type);
    delete item.params;

    // Serialize objects using their id
    item.constructorParams = serializeParams(item.constructorParams);

    rpc.encode('create', item, function(error, result)
    {
      if(error) return callback(error);

      var id = result.value;

      var mediaObject = objects[id];
      if(mediaObject) return callback(null, mediaObject);

      callback(null, createObject(constructor, id));
    });
  };

  function describe(id, callback)
  {
    var mediaObject = objects[id];
    if(mediaObject) return callback(null, mediaObject);

    rpc.encode('describe', {object: id}, function(error, result)
    {
      if(error) return callback(error);

      var type = result.type;

      var constructor = getConstructor(type);

      if(constructor.create)
      {
        item = constructor.create(item.params);

        // Apply inheritance
        var prototype = constructor.prototype;
        inherits(constructor, getConstructor(item.type));
        extend(constructor.prototype, prototype);
      };

      return callback(null, createObject(constructor, id));
    });
  };


  this.getMediaobjectById = function(id, callback)
  {
    var promise = new Promise(function(resolve, reject)
    {
      function callback(error, result)
      {
        if(error) return reject(error);

        resolve(result);
      };

      if(id instanceof Array)
        async.map(id, describe, callback);
      else
        describe(id, callback);
    });

    promiseCallback(promise, callback);

    return promise;
  };


  /**
   * Create a new instance of a MediaObject
   *
   * @param {external:String} type - Type of the element
   * @param {external:string[]} [params]
   * @callback {createMediaPipelineCallback} callback
   *
   * @return {module:kwsMediaApi~MediaPipeline} The pipeline itself
   */
  this.create = function(type, params, callback)
  {
    // Fix optional parameters
    if(params instanceof Function)
    {
      if(callback)
        throw new SyntaxError("Nothing can be defined after the callback");

      callback = params;
      params   = undefined;
    };

    var promise = new Promise(function(resolve, reject)
    {
      function callback(error, result)
      {
        if(error) return reject(error);

        resolve(result);
      };

      if(type instanceof Array)
        async.map(type, createMediaObject, callback);
      else
        createMediaObject({params: params || {}, type: type}, callback);
    });

    promiseCallback(promise, callback);

    return promise;
  };
};
inherits(KwsMedia, EventEmitter);


/**
 * Connect the source of a media to the sink of the next one
 *
 * @param {...MediaObject} media - A media to be connected
 * @callback {createMediaObjectCallback} [callback]
 *
 * @return {module:kwsMediaApi~MediaPipeline} The pipeline itself
 *
 * @throws {SyntaxError}
 */
KwsMedia.prototype.connect = function(media, callback)
{
  // Fix lenght-variable arguments
  media = Array.prototype.slice.call(arguments, 0);
  callback = (typeof media[media.length - 1] == 'function') ? media.pop() : noop;

  // Check if we have enought media components
  if(media.length < 2)
    throw new SyntaxError("Need at least two media elements to connect");

  // Check MediaElements are of the correct type
  media.forEach(function(element)
  {
    checkType('MediaElement', 'media', element);
  });

  // Generate promise
  var promise = new Promise(function(resolve, reject)
  {
    function callback(error, result)
    {
      if(error) return reject(error);

      resolve(result);
    };

    // Connect the media elements
    var src = media[0];

    async.each(media.slice(1), function(sink, callback)
    {
      src.connect(sink, callback);
      src = sink;
    }, callback);
  });

  promiseCallback(promise, callback);

  return promise;
};


/**
 * The built in number object.
 * @external Number
 * @see {@link https://developer.mozilla.org/en/JavaScript/Reference/Global_Objects/Number Number}
 */

/**
 * The built in string object.
 * @external String
 * @see {@link https://developer.mozilla.org/en/JavaScript/Reference/Global_Objects/String String}
 */


/**
 * Node.js EventEmitter object.
 * @external EventEmitter
 * @see {@link http://nodejs.org/api/events.html#events_class_events_eventemitter EventEmitter}
 */


module.exports = KwsMedia;

},{"./checkType":3,"./core":22,"./core/Filter":13,"./core/Hub":14,"./core/MediaElement":16,"./core/MediaObject":17,"./core/MediaPad":18,"./core/MediaSink":20,"./core/MediaSource":21,"./endpoints":33,"./endpoints/HttpEndpoint":23,"./endpoints/SdpEndpoint":29,"./endpoints/SessionEndpoint":30,"./endpoints/UriEndpoint":31,"./filters":43,"./hubs":47,"./utils":48,"async":49,"es6-promise":50,"events":61,"extend":60,"inherits":68,"kws-rpc-builder":72,"url":67,"ws":76}],2:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var EventEmitter = require('events').EventEmitter;

var inherits = require('inherits');

var Promise = require('es6-promise').Promise;

var utils = require('./utils');

var noop            = utils.noop;
var promiseCallback = utils.promiseCallback;


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */


/**
 * Internal base class for all objects that can be created in the media server.
 *
 * @abstract
 * @class   module:kwsMediaApi~_MediaObject
 * @extends external:EventEmitter
 */

/**
 *
 * @constructor
 *
 * @param {string} id
 */
function _MediaObject(id)
{
  var self = this;

  EventEmitter.call(this);


  //
  // Define object properties
  //

  /**
   * Unique identifier of this object
   *
   * @public
   * @readonly
   * @member {external:Number}
   */
  Object.defineProperty(this, "id", {value: id});


  //
  // Subscribe and unsubscribe events on the server when adding and removing
  // event listeners on this MediaObject
  //

  var subscriptions = {};

  this.on('removeListener', function(event, listener)
  {
    // Blacklisted events
    if(event == 'release'
    || event == '_rpc'
    || event == 'newListener')
      return;

    var count = EventEmitter.listenerCount(self, event);
    if(count) return;

    var token = subscriptions[event];

    this.emit('_rpc', 'unsubscribe', {subscription: token}, function(error)
    {
      if(error) return self.emit('error', error);

      delete subscriptions[event];
    });
  });

  this.on('newListener', function(event, listener)
  {
    // Blacklisted events
    if(event == 'release'
    || event == '_rpc'
    || event == '_create')
      return;

    var count = EventEmitter.listenerCount(self, event);
    if(count) return;

    this.emit('_rpc', 'subscribe', {type: event}, function(error, token)
    {
      if(error) return self.emit('error', error);

      subscriptions[event] = token;
    });
  });
};
inherits(_MediaObject, EventEmitter);


/**
 * Send a command to a media object
 *
 * @param {external:String} method - Command to be executed by the server
 * @param {module:kwsMediaApi~_MediaObject.constructorParams} [params]
 * @callback {invokeCallback} callback
 *
 * @return {module:kwsMediaApi~_MediaObject} The own media object
 */
_MediaObject.prototype.invoke = function(method, params, callback){
  var self = this;

  // Fix optional parameters
  if(params instanceof Function)
  {
    if(callback)
      throw new SyntaxError("Nothing can be defined after the callback");

    callback = params;
    params = undefined;
  };

  var promise = new Promise(function(resolve, reject)
  {
    // Generate request parameters
    var params2 =
    {
      operation: method
    };

    if(params)
      params2.operationParams = params;

    // Do request
    self.emit('_rpc', 'invoke', params2, function(error, result)
    {
      if(error) return reject(error);

      var value = result.value;
      if(value === undefined) value = self;

      resolve(value);
    });
  });

  promiseCallback(promise, callback);

  return promise;
};
/**
 * @callback invokeCallback
 * @param {MediaServerError} error
 */

/**
 * Explicity release a {@link module:kwsMediaApi~_MediaObject MediaObject} from memory
 *
 * All its descendants will be also released and collected
 *
 * @throws {module:kwsMediaApi~MediaServerError}
 */
_MediaObject.prototype.release = function(callback){
  var self = this;

  var promise = new Promise(function(resolve, reject)
  {
    self.emit('_rpc', 'release', {}, function(error)
    {
      if(error) return reject(error);

      self.emit('release');

      // Remove events on the object and remove object from cache
      self.removeAllListeners();

      resolve();
    });
  });

  promiseCallback(promise, callback);

  return promise;
};


module.exports = _MediaObject;

},{"./utils":48,"es6-promise":50,"events":61,"inherits":68}],3:[function(require,module,exports){
/*
 * (C) Copyright 2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var extend = require('extend');

/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi
 *
 * @copyright 2014 Kurento (http://kurento.org/)
 * @license LGPL
 */


/**
 * Number.isInteger() polyfill
 * @function external:Number#isInteger
 * @see {@link https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Number/isInteger Number.isInteger}
 */
if (!Number.isInteger) {
  Number.isInteger = function isInteger (nVal) {
    return typeof nVal === "number" && isFinite(nVal) && nVal > -9007199254740992 && nVal < 9007199254740992 && Math.floor(nVal) === nVal;
  };
}


//
// Basic types
//

function checkArray(type, key, value)
{
  if(!(value instanceof Array))
    throw SyntaxError(key+' param should be an Array of '+type+', not '+typeof value);

  for(var i=0, item; item=value[i]; i++)
    checkType(type, key+'['+i+']', item);
};

function checkBoolean(key, value)
{
  if(typeof value != 'boolean')
    throw SyntaxError(key+' param should be a Boolean, not '+typeof value);
};

function checkNumber(key, value)
{
  if(typeof value != 'number')
    throw SyntaxError(key+' param should be a Number, not '+typeof value);
};

function checkInteger(key, value)
{
  if(!Number.isInteger(value))
    throw SyntaxError(key+' param should be an Integer, not '+typeof value);
};

function checkObject(key, value)
{
  if(typeof value != 'object')
    throw SyntaxError(key+' param should be an Object, not '+typeof value);
};

function checkString(key, value)
{
  if(typeof value != 'string')
    throw SyntaxError(key+' param should be a String, not '+typeof value);
};


// Checker functions

function checkType(type, key, value, options)
{
  options = options || {};

  if(value != undefined)
  {
    if(options.isArray)
      return checkArray(type, key, value);

    var checker = checkType[type];
    if(checker) return checker(key, value);

    console.warn("Could not check "+key+", unknown type "+type);
//    throw TypeError("Could not check "+key+", unknown type "+type);
  }

  else if(options.required)
    throw SyntaxError(key+" param is required");

};

function checkParams(params, scheme, class_name)
{
  var result = {};

  // check MediaObject params
  for(var key in scheme)
  {
    var value = params[key];

    var s = scheme[key];

    var options = {required: s.required, isArray: s.isList};

    checkType(s.type, key, value, options);

    if(value == undefined) continue;

    result[key] = value;
    delete params[key];
  };

  if(Object.keys(params).length)
    console.warn('Unused params for '+class_name+':', params);

  return result;
};

function checkMethodParams(callparams, method_params)
{
  var result = {};

  var index=0, param;
  for(; param=method_params[index]; index++)
  {
    var key = param.name;
    var value = callparams[index];

    var options = {required: param.required, isArray: param.isList};

    checkType(param.type, key, value, options);

    result[key] = value;
  }

  var params = callparams.slice(index);
  if(params.length)
    console.warning('Unused params:', params);

  return result;
};


module.exports = checkType;

checkType.checkParams = checkParams;


// Basic types

checkType.boolean = checkBoolean;
checkType.double  = checkNumber;
checkType.float   = checkNumber;
checkType.int     = checkInteger;
checkType.Object  = checkObject;
checkType.String  = checkString;


// Complex types

var complexTypes = require('./complexTypes');

extend(checkType, complexTypes);


// Elements

function addCheckers(elements)
{
  for(var key in elements)
  {
    var check = elements[key].check;
    if(check) checkType[key] = check;
  };
};


var core      = require('./core');
var endpoints = require('./endpoints');
var filters   = require('./filters');
var hubs      = require('./hubs');

addCheckers(core);
addCheckers(endpoints);
addCheckers(filters);
addCheckers(hubs);


var MediaElement = require('./core/MediaElement');

checkType.MediaElement = MediaElement.check;

},{"./complexTypes":11,"./core":22,"./core/MediaElement":16,"./endpoints":33,"./filters":43,"./hubs":47,"extend":60}],4:[function(require,module,exports){
/* Autogenerated with Kurento Idl */

/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var checkType = require('../checkType');

/**
 * Media API for the Kurento Web SDK
 *
 * @module KwsMedia/complexTypes
 *
 * @copyright 2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

/**
 * Checker for {@link KwsMedia/complexTypes~MediaProfileSpecType}
 *
 * @param {String} key
 * @param {KwsMedia/complexTypes~MediaProfileSpecType} value
 */
function checkMediaProfileSpecType(key, value)
{
  if(typeof value != 'string')
    throw SyntaxError(key+' param should be a String, not '+typeof value);
  if(!value.match('WEBM|MP4'))
    throw SyntaxError(key+' param is not one of [WEBM|MP4] ('+value+')');
};


/**
 * Media Profile.

Currently WEBM and MP4 are supported.
 *
 * @typedef KwsMedia/complexTypes~MediaProfileSpecType
 *
 * @type {WEBM|MP4}
 */


module.exports = checkMediaProfileSpecType;

},{"../checkType":3}],5:[function(require,module,exports){
/* Autogenerated with Kurento Idl */

/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var checkType = require('../checkType');

/**
 * Media API for the Kurento Web SDK
 *
 * @module KwsMedia/complexTypes
 *
 * @copyright 2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

/**
 * Checker for {@link KwsMedia/complexTypes~MediaType}
 *
 * @param {String} key
 * @param {KwsMedia/complexTypes~MediaType} value
 */
function checkMediaType(key, value)
{
  if(typeof value != 'string')
    throw SyntaxError(key+' param should be a String, not '+typeof value);
  if(!value.match('AUDIO|DATA|VIDEO'))
    throw SyntaxError(key+' param is not one of [AUDIO|DATA|VIDEO] ('+value+')');
};


/**
 * Type of media stream to be exchanged.
Can take the values AUDIO, DATA or VIDEO.
 *
 * @typedef KwsMedia/complexTypes~MediaType
 *
 * @type {AUDIO|DATA|VIDEO}
 */


module.exports = checkMediaType;

},{"../checkType":3}],6:[function(require,module,exports){
/* Autogenerated with Kurento Idl */

/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var checkType = require('../checkType');

/**
 * Media API for the Kurento Web SDK
 *
 * @module KwsMedia/complexTypes
 *
 * @copyright 2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

/**
 * Checker for {@link KwsMedia/complexTypes~Point}
 *
 * @param {String} key
 * @param {KwsMedia/complexTypes~Point} value
 */
function checkPoint(key, value)
{
  checkType('int', key+'.x', value.x, true);
  checkType('int', key+'.y', value.y, true);
};


/**
 * Point in a physical screen, coordinates are in pixels with X left to right and Y top to down.
 *
 * @typedef KwsMedia/complexTypes~Point
 *
 * @type {Object}
 * @property {int} x - X coordinate in pixels of a point in the screen
 * @property {int} y - Y coordinate in pixels of a point in the screen
 */


module.exports = checkPoint;

},{"../checkType":3}],7:[function(require,module,exports){
/* Autogenerated with Kurento Idl */

/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var checkType = require('../checkType');

/**
 * Media API for the Kurento Web SDK
 *
 * @module KwsMedia/complexTypes
 *
 * @copyright 2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

/**
 * Checker for {@link KwsMedia/complexTypes~PointerDetectorWindowMediaParam}
 *
 * @param {String} key
 * @param {KwsMedia/complexTypes~PointerDetectorWindowMediaParam} value
 */
function checkPointerDetectorWindowMediaParam(key, value)
{
  checkType('String', key+'.id', value.id, true);
  checkType('int', key+'.height', value.height, true);
  checkType('int', key+'.width', value.width, true);
  checkType('int', key+'.upperRightX', value.upperRightX, true);
  checkType('int', key+'.upperRightY', value.upperRightY, true);
  checkType('String', key+'.activeImage', value.activeImage);
  checkType('float', key+'.imageTransparency', value.imageTransparency);
  checkType('String', key+'.image', value.image);
};


/**
 * Data structure for UI Pointer detection in video streams.

All the coordinates are in pixels. X is horizontal, Y is vertical, running from the top of the window. Thus, 0,0 corresponds to the topleft corner.
 *
 * @typedef KwsMedia/complexTypes~PointerDetectorWindowMediaParam
 *
 * @type {Object}
 * @property {String} id - id of the window for pointer detection
 * @property {int} height - height in pixels
 * @property {int} width - width in pixels
 * @property {int} upperRightX - X coordinate in pixels of the upper left corner
 * @property {int} upperRightY - Y coordinate in pixels of the upper left corner
 * @property {String} activeImage - uri of the image to be used when the pointer is inside the window
 * @property {float} imageTransparency - transparency ratio of the image
 * @property {String} image - uri of the image to be used for the window.

If :rom:attr:`activeImage` has been set, it will only be shown when the pointer is outside of the window.
 */


module.exports = checkPointerDetectorWindowMediaParam;

},{"../checkType":3}],8:[function(require,module,exports){
/* Autogenerated with Kurento Idl */

/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var checkType = require('../checkType');

/**
 * Media API for the Kurento Web SDK
 *
 * @module KwsMedia/complexTypes
 *
 * @copyright 2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

/**
 * Checker for {@link KwsMedia/complexTypes~RegionOfInterest}
 *
 * @param {String} key
 * @param {KwsMedia/complexTypes~RegionOfInterest} value
 */
function checkRegionOfInterest(key, value)
{
  checkType('Point', key+'.points', value.points, true);
  checkType('RegionOfInterestConfig', key+'.regionOfInterestConfig', value.regionOfInterestConfig, true);
  checkType('String', key+'.id', value.id, true);
};


/**
 * Region of interest for some events in a video processing filter
 *
 * @typedef KwsMedia/complexTypes~RegionOfInterest
 *
 * @type {Object}
 * @property {Point} points - list of points delimiting the region of interest
 * @property {RegionOfInterestConfig} regionOfInterestConfig - data structure for configuration of CrowdDetector regions of interest
 * @property {String} id - identifier of the region of interest. The string used for the id must begin 
 with a letter followed by an alphanumeric character included (/-_.:+)
 */


module.exports = checkRegionOfInterest;

},{"../checkType":3}],9:[function(require,module,exports){
/* Autogenerated with Kurento Idl */

/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var checkType = require('../checkType');

/**
 * Media API for the Kurento Web SDK
 *
 * @module KwsMedia/complexTypes
 *
 * @copyright 2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

/**
 * Checker for {@link KwsMedia/complexTypes~RegionOfInterestConfig}
 *
 * @param {String} key
 * @param {KwsMedia/complexTypes~RegionOfInterestConfig} value
 */
function checkRegionOfInterestConfig(key, value)
{
  checkType('int', key+'.occupancyLevelMin', value.occupancyLevelMin);
  checkType('int', key+'.occupancyLevelMed', value.occupancyLevelMed);
  checkType('int', key+'.occupancyLevelMax', value.occupancyLevelMax);
  checkType('int', key+'.occupancyNumFramesToEvent', value.occupancyNumFramesToEvent);
  checkType('int', key+'.fluidityLevelMin', value.fluidityLevelMin);
  checkType('int', key+'.fluidityLevelMed', value.fluidityLevelMed);
  checkType('int', key+'.fluidityLevelMax', value.fluidityLevelMax);
  checkType('int', key+'.fluidityNumFramesToEvent', value.fluidityNumFramesToEvent);
  checkType('boolean', key+'.sendOpticalFlowEvent', value.sendOpticalFlowEvent);
  checkType('int', key+'.opticalFlowNumFramesToEvent', value.opticalFlowNumFramesToEvent);
  checkType('int', key+'.opticalFlowNumFramesToReset', value.opticalFlowNumFramesToReset);
  checkType('int', key+'.opticalFlowAngleOffset', value.opticalFlowAngleOffset);
};


/**
 * data structure for configuration of CrowdDetector regions of interest
 *
 * @typedef KwsMedia/complexTypes~RegionOfInterestConfig
 *
 * @type {Object}
 * @property {int} occupancyLevelMin - minimun occupancy percentage in the ROI to send occupancy events
 * @property {int} occupancyLevelMed - send occupancy level = 1 if the occupancy percentage is between occupancy_level_min and this level
 * @property {int} occupancyLevelMax - send occupancy level = 2 if the occupancy percentage is between occupancy_level_med and this level,
and send occupancy level = 3 if the occupancy percentage is between this level and 100
 * @property {int} occupancyNumFramesToEvent - number of consecutive frames that a new occupancy level has to be detected to recognize it as a occupancy level change.
A new occupancy event will be send
 * @property {int} fluidityLevelMin - minimun fluidity percentage in the ROI to send fluidity events
 * @property {int} fluidityLevelMed - send fluidity level = 1 if the fluidity percentage is between fluidity_level_min and this level
 * @property {int} fluidityLevelMax - send fluidity level = 2 if the fluidity percentage is between fluidity_level_med and this level,
 and send fluidity level = 3 if the fluidity percentage is between this level and 100
 * @property {int} fluidityNumFramesToEvent - number of consecutive frames that a new fluidity level has to be detected to recognize it as a fluidity level change.
 A new fluidity event will be send
 * @property {boolean} sendOpticalFlowEvent - Enable/disable the movement direction detection into the ROI
 * @property {int} opticalFlowNumFramesToEvent - number of consecutive frames that a new direction of movement has to be detected to recognize a new movement direction. 
 A new direction event will be send
 * @property {int} opticalFlowNumFramesToReset - number of consecutive frames in order to reset the counter of repeated directions
 * @property {int} opticalFlowAngleOffset - Direction of the movement. The angle could have four different values: 
 left (0), up (90), right (180) and down (270). This cartesian axis could be rotated adding an angle offset
 */


module.exports = checkRegionOfInterestConfig;

},{"../checkType":3}],10:[function(require,module,exports){
/* Autogenerated with Kurento Idl */

/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var checkType = require('../checkType');

/**
 * Media API for the Kurento Web SDK
 *
 * @module KwsMedia/complexTypes
 *
 * @copyright 2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

/**
 * Checker for {@link KwsMedia/complexTypes~WindowParam}
 *
 * @param {String} key
 * @param {KwsMedia/complexTypes~WindowParam} value
 */
function checkWindowParam(key, value)
{
  checkType('int', key+'.topRightCornerX', value.topRightCornerX, true);
  checkType('int', key+'.topRightCornerY', value.topRightCornerY, true);
  checkType('int', key+'.width', value.width, true);
  checkType('int', key+'.height', value.height, true);
};


/**
 * Parameter representing a window in a video stream.
It is used in command and constructors for media elements.

All units are in pixels, X runs from left to right, Y from top to bottom.
 *
 * @typedef KwsMedia/complexTypes~WindowParam
 *
 * @type {Object}
 * @property {int} topRightCornerX - X coordinate of the left upper point of the window
 * @property {int} topRightCornerY - Y coordinate of the left upper point of the window
 * @property {int} width - width in pixels of the window
 * @property {int} height - height in pixels of the window
 */


module.exports = checkWindowParam;

},{"../checkType":3}],11:[function(require,module,exports){
/*
 * (C) Copyright 2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/complexTypes
 *
 * @copyright 2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var MediaProfileSpecType = require('./MediaProfileSpecType');
var MediaType = require('./MediaType');
var Point = require('./Point');
var PointerDetectorWindowMediaParam = require('./PointerDetectorWindowMediaParam');
var RegionOfInterest = require('./RegionOfInterest');
var RegionOfInterestConfig = require('./RegionOfInterestConfig');
var WindowParam = require('./WindowParam');


exports.MediaProfileSpecType = MediaProfileSpecType;
exports.MediaType = MediaType;
exports.Point = Point;
exports.PointerDetectorWindowMediaParam = PointerDetectorWindowMediaParam;
exports.RegionOfInterest = RegionOfInterest;
exports.RegionOfInterestConfig = RegionOfInterestConfig;
exports.WindowParam = WindowParam;

},{"./MediaProfileSpecType":4,"./MediaType":5,"./Point":6,"./PointerDetectorWindowMediaParam":7,"./RegionOfInterest":8,"./RegionOfInterestConfig":9,"./WindowParam":10}],12:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/core
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var MediaElement = require('./MediaElement');


/**
 * Base interface for all end points. An Endpoint is a :rom:cls:`MediaElement`
that allow :term:`KMS` to interchange media contents with external systems,
supporting different transport protocols and mechanisms, such as :term:`RTP`,
:term:`WebRTC`, :term:`HTTP`, ``file:/`` URLs... An ``Endpoint`` may
contain both sources and sinks for different media types, to provide
bidirectional communication.
 *
 * @abstract
 * @class   module:kwsMediaApi/core~Endpoint
 * @extends module:kwsMediaApi~MediaElement
 */

/**
 * @constructor
 *
 * @param {string} id
 */
function Endpoint(id)
{
  MediaElement.call(this, id);
};
inherits(Endpoint, MediaElement);


/**
 * @type module:kwsMediaApi/core~Endpoint.constructorParams
 */
Endpoint.constructorParams = {};

/**
 * @type   module:kwsMediaApi/core~Endpoint.events
 * @extend module:kwsMediaApi~MediaElement.events
 */
Endpoint.events = [];
Endpoint.events.concat(MediaElement.events);


module.exports = Endpoint;


Endpoint.check = function(key, value)
{
  if(!(value instanceof Endpoint))
    throw SyntaxError(key+' param should be a Endpoint, not '+typeof value);
};

},{"./MediaElement":16,"inherits":68}],13:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/core
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var MediaElement = require('./MediaElement');


/**
 * Base interface for all filters. This is a certain type of :rom:cls:`MediaElement`, that processes media injected through its :rom:cls:`MediaSink`, and delivers the outcome through its :rom:cls:`MediaSource`.
 *
 * @abstract
 * @class   module:kwsMediaApi/core~Filter
 * @extends module:kwsMediaApi~MediaElement
 */

/**
 * @constructor
 *
 * @param {string} id
 */
function Filter(id)
{
  MediaElement.call(this, id);
};
inherits(Filter, MediaElement);


/**
 * @type module:kwsMediaApi/core~Filter.constructorParams
 */
Filter.constructorParams = {};

/**
 * @type   module:kwsMediaApi/core~Filter.events
 * @extend module:kwsMediaApi~MediaElement.events
 */
Filter.events = [];
Filter.events.concat(MediaElement.events);


module.exports = Filter;


Filter.check = function(key, value)
{
  if(!(value instanceof Filter))
    throw SyntaxError(key+' param should be a Filter, not '+typeof value);
};

},{"./MediaElement":16,"inherits":68}],14:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var Promise = require('es6-promise').Promise;

var promiseCallback = require('../utils').promiseCallback;


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/core
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var MediaObject = require('./MediaObject');


/**
 * A Hub is a routing :rom:cls:`MediaObject`. It connects several :rom:cls:`endpoints <Endpoint>` together
 *
 * @abstract
 * @class   module:kwsMediaApi/core~Hub
 * @extends module:kwsMediaApi~MediaObject
 */

/**
 * @constructor
 *
 * @param {string} id
 */
function Hub(id)
{
  MediaObject.call(this, id);
};
inherits(Hub, MediaObject);


/**
 * Create a new instance of a {module:kwsMediaApi/core~HubPort} attached to this {module:kwsMediaApi/core~Hub}
 *
 * @callback {createHubCallback} callback
 *
 * @return {module:kwsMediaApi/core~Hub} The hub itself
 */
Hub.prototype.createHubPort = function(callback)
{
  var self = this;

  var promise = new Promise(function(resolve, reject)
  {
    self.emit('_create', 'HubPort', {hub: self}, function(error, result)
    {
      if(error) return reject(error);

      resolve(result);
    });
  });

  promiseCallback(promise, callback);

  return promise;
};
/**
 * @callback module:kwsMediaApi/core~Hub~createHubCallback
 * @param {Error} error
 * @param {module:kwsMediaApi/core~HubPort} result
 *  The created HubPort
 */


/**
 * @type module:kwsMediaApi/core~Hub.constructorParams
 */
Hub.constructorParams = {};

/**
 * @type   module:kwsMediaApi/core~Hub.events
 * @extend module:kwsMediaApi~MediaObject.events
 */
Hub.events = [];
Hub.events.concat(MediaObject.events);


module.exports = Hub;


Hub.check = function(key, value)
{
  if(!(value instanceof Hub))
    throw SyntaxError(key+' param should be a Hub, not '+typeof value);
};

},{"../utils":48,"./MediaObject":17,"es6-promise":50,"inherits":68}],15:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/core
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var MediaElement = require('./MediaElement');


/**
 * This :rom:cls:`MediaElement` specifies a connection with a :rom:cls:`Hub`
 *
 * @class   module:kwsMediaApi/core~HubPort
 * @extends module:kwsMediaApi~MediaElement
 */

/**
 * Creates a :rom:cls:`HubPort` for the given :rom:cls:`Hub`
 *
 * @constructor
 *
 * @param {string} id
 */
function HubPort(id)
{
  MediaElement.call(this, id);
};
inherits(HubPort, MediaElement);


/**
 * @type module:kwsMediaApi/core~HubPort.constructorParams
 *
 * @property {Hub} hub
 *  :rom:cls:`Hub` to which this port belongs
 */
HubPort.constructorParams = {
  hub: {
    type: 'Hub',
    required: true
  },
};

/**
 * @type   module:kwsMediaApi/core~HubPort.events
 * @extend module:kwsMediaApi~MediaElement.events
 */
HubPort.events = [];
HubPort.events.concat(MediaElement.events);


module.exports = HubPort;


HubPort.check = function(key, value)
{
  if(!(value instanceof HubPort))
    throw SyntaxError(key+' param should be a HubPort, not '+typeof value);
};

},{"./MediaElement":16,"inherits":68}],16:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var checkType = require('../checkType');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/core
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var MediaObject = require('./MediaObject');


/**
 * Basic building blocks of the media server, that can be interconnected through the API. A :rom:cls:`MediaElement` is a module that encapsulates a specific media capability. They can be connected to create media pipelines where those capabilities are applied, in sequence, to the stream going through the pipeline.

   :rom:cls:`MediaElement` objects are classified by its supported media type (audio, video, etc.) and the flow direction: :rom:cls:`MediaSource` pads are intended for media delivery while :rom:cls:`MediaSinks<MediaSink>`  behave as reception points.
 *
 * @abstract
 * @class   module:kwsMediaApi/core~MediaElement
 * @extends module:kwsMediaApi~MediaObject
 */

/**
 * @constructor
 *
 * @param {string} id
 */
function MediaElement(id)
{
  MediaObject.call(this, id);
};
inherits(MediaElement, MediaObject);


/**
 * perform :rom:meth:`connect(sink,mediaType)` if there is exactly one sink for the given type, and their mediaDescriptions are the same
 *
 * @param {MediaElement} sink
 *  the target :rom:cls:`MediaElement`  from which :rom:cls:`MediaSink` will be obtained
 *
 * @param {MediaPad.MediaType} [mediaType]
 *  the :rom:enum:`MediaType` of the pads that will be connected
 *
 * @param {external:String} [mediaDescription]
 *  A textual description of the media source. Currently not used, aimed mainly for :rom:attr:`MediaType.DATA` sources
 *
 * @param {module:kwsMediaApi/core~MediaElement.connectCallback} [callback]
 *
 * @return {module:kwsMediaApi/core~MediaElement}
 *  The own media object
 */
MediaElement.prototype.connect = function(sink, mediaType, mediaDescription, callback){
  // Fix optional parameters
  if(mediaType instanceof Function)
  {
    if(mediaDescription)
      throw new SyntaxError("Nothing can be defined after the callback");

    callback = mediaType;
    mediaDescription = undefined;
    mediaType = undefined;
  }

  else if(mediaDescription instanceof Function)
  {
    if(callback)
      throw new SyntaxError("Nothing can be defined after the callback");

    callback = mediaDescription;
    mediaDescription = undefined;
  };

  if(!mediaType && mediaDescription)
    throw new SyntaxError("'mediaType' is undefined while 'mediaDescription' is not");

  checkType('MediaElement', 'sink', sink, {required: true});
  checkType('MediaType', 'mediaType', mediaType);
  checkType('String', 'mediaDescription', mediaDescription);

  var params = {
    sink: sink,
    mediaType: mediaType,
    mediaDescription: mediaDescription,
  };

  return this.invoke('connect', params, callback);
};
/**
 * @callback MediaElement~connectCallback
 * @param {Error} error
 */

/**
 * A list of sinks of the given :rom:ref:`MediaType`. The list will be empty if no sinks are found.
 *
 * @param {MediaPad.MediaType} [mediaType]
 *  One of :rom:attr:`MediaType.AUDIO`, :rom:attr:`MediaType.VIDEO` or :rom:attr:`MediaType.DATA`
 *
 * @param {external:String} [description]
 *  A textual description of the media source. Currently not used, aimed mainly for :rom:attr:`MediaType.DATA` sources
 *
 * @param {module:kwsMediaApi/core~MediaElement.getMediaSinksCallback} [callback]
 *
 * @return {module:kwsMediaApi/core~MediaElement}
 *  The own media object
 */
MediaElement.prototype.getMediaSinks = function(mediaType, description, callback){
  // Fix optional parameters
  if(mediaType instanceof Function)
  {
    if(description)
      throw new SyntaxError("Nothing can be defined after the callback");

    callback = mediaType;
    description = undefined;
    mediaType = undefined;
  }

  else if(description instanceof Function)
  {
    if(callback)
      throw new SyntaxError("Nothing can be defined after the callback");

    callback = description;
    description = undefined;
  };

  if(!mediaType && description)
    throw new SyntaxError("'mediaType' is undefined while 'description' is not");

  checkType('MediaType', 'mediaType', mediaType);
  checkType('String', 'description', description);

  var params = {
    mediaType: mediaType,
    description: description,
  };

  return this.invoke('getMediaSinks', params, callback);
};
/**
 * @callback MediaElement~getMediaSinksCallback
 * @param {Error} error
 * @param {MediaSink} result
 *  A list of sinks. The list will be empty if no sinks are found.
 */

/**
 * Get the media sources of the given type and description
 *
 * @param {MediaPad.MediaType} [mediaType]
 *  One of :rom:attr:`MediaType.AUDIO`, :rom:attr:`MediaType.VIDEO` or :rom:attr:`MediaType.DATA`
 *
 * @param {external:string} [description]
 *  A textual description of the media source. Currently not used, aimed mainly for :rom:attr:`MediaType.DATA` sources
 *
 * @param {module:kwsMediaApi/core~MediaElement.getMediaSrcsCallback} [callback]
 *
 * @return {module:kwsMediaApi/core~MediaElement}
 *  The own media object
 */
MediaElement.prototype.getMediaSrcs = function(mediaType, description, callback){
  // Fix optional parameters
  if(mediaType instanceof Function)
  {
    if(description)
      throw new SyntaxError("Nothing can be defined after the callback");

    callback = mediaType;
    description = undefined;
    mediaType = undefined;
  }

  else if(description instanceof Function)
  {
    if(callback)
      throw new SyntaxError("Nothing can be defined after the callback");

    callback = description;
    description = undefined;
  };

  if(!mediaType && description)
    throw new SyntaxError("'mediaType' is undefined while 'description' is not");

  checkType('MediaType', 'mediaType', mediaType);
  checkType('String', 'description', description);

  var params = {
    mediaType: mediaType,
    description: description,
  };

  return this.invoke('getMediaSrcs', params, callback);
};
/**
 * @callback MediaElement~getMediaSrcsCallback
 * @param {Error} error
 * @param {MediaSource} result
 *  A list of sources. The list will be empty if no sources are found.
 */


/**
 * @type module:kwsMediaApi/core~MediaElement.constructorParams
 */
MediaElement.constructorParams = {};

/**
 * @type   module:kwsMediaApi/core~MediaElement.events
 * @extend module:kwsMediaApi~MediaObject.events
 */
MediaElement.events = [];
MediaElement.events.concat(MediaObject.events);


module.exports = MediaElement;


MediaElement.check = function(key, value)
{
  if(!(value instanceof MediaElement))
    throw SyntaxError(key+' param should be a MediaElement, not '+typeof value);
};

},{"../checkType":3,"./MediaObject":17,"inherits":68}],17:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var checkType = require('../checkType');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/core
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var _MediaObject = require('../_MediaObject');


/**
 * Base for all objects that can be created in the media server.
 *
 * @abstract
 * @class   module:kwsMediaApi/core~MediaObject
 */

/**
 * @constructor
 *
 * @param {string} id
 */
function MediaObject(id)
{
  _MediaObject.call(this, id);
};
inherits(MediaObject, _MediaObject);


/**
 * Returns the pipeline to which this MediaObject belong, or the pipeline itself if invoked over a :rom:cls:`MediaPipeline`
 *
 * @param {module:kwsMediaApi/core~MediaObject.getMediaPipelineCallback} [callback]
 *
 * @return {module:kwsMediaApi/core~MediaObject}
 *  The own media object
 */
MediaObject.prototype.getMediaPipeline = function(callback){
  return this.invoke('getMediaPipeline', callback);
};
/**
 * @callback MediaObject~getMediaPipelineCallback
 * @param {Error} error
 * @param {MediaPipeline} result
 *  the MediaPipeline this MediaObject belongs to.

If called on a :rom:cls:`MediaPipeline` it will return ``this``.
 */

/**
 * Returns the parent of this media object. The type of the parent depends on the type of the element that this method is called upon:

The parent of a :rom:cls:`MediaPad` is its :rom:cls:`MediaElement`; the parent of a :rom:cls:`MediaMixer` or a :rom:cls:`MediaElement` is its :rom:cls:`MediaPipeline`.

A :rom:cls:`MediaPipeline` has no parent, i.e. the method returns null
 *
 * @param {module:kwsMediaApi/core~MediaObject.getParentCallback} [callback]
 *
 * @return {module:kwsMediaApi/core~MediaObject}
 *  The own media object
 */
MediaObject.prototype.getParent = function(callback){
  return this.invoke('getParent', callback);
};
/**
 * @callback MediaObject~getParentCallback
 * @param {Error} error
 * @param {MediaObject} result
 *  the parent of this MediaObject or null if called on a MediaPipeline
 */


/**
 * @type module:kwsMediaApi/core~MediaObject.constructorParams
 */
MediaObject.constructorParams = {};

/**
 * @type module:kwsMediaApi/core~MediaObject.events
 */
MediaObject.events = ['Error'];


module.exports = MediaObject;


MediaObject.check = function(key, value)
{
  if(!(value instanceof MediaObject))
    throw SyntaxError(key+' param should be a MediaObject, not '+typeof value);
};

},{"../_MediaObject":2,"../checkType":3,"inherits":68}],18:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var checkType = require('../checkType');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/core
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var MediaObject = require('./MediaObject');


/**
 * A :rom:cls:`MediaPad` is an element´s interface with the outside world. The data streams flow from the :rom:cls:`MediaSource` pad to another element's :rom:cls:`MediaSink` pad.
 *
 * @abstract
 * @class   module:kwsMediaApi/core~MediaPad
 * @extends module:kwsMediaApi~MediaObject
 */

/**
 * @constructor
 *
 * @param {string} id
 */
function MediaPad(id)
{
  MediaObject.call(this, id);
};
inherits(MediaPad, MediaObject);


/**
 * Obtains the description for this pad.

   This method does not make a request to the media server, and is included to keep the simmetry with the rest of methods from the API.
 *
 * @param {module:kwsMediaApi/core~MediaPad.getMediaDescriptionCallback} [callback]
 *
 * @return {module:kwsMediaApi/core~MediaPad}
 *  The own media object
 */
MediaPad.prototype.getMediaDescription = function(callback){
  return this.invoke('getMediaDescription', callback);
};
/**
 * @callback MediaPad~getMediaDescriptionCallback
 * @param {Error} error
 * @param {String} result
 *  The description
 */

/**
 * Obtains the :rom:cls:`MediaElement` that encloses this pad
 *
 * @param {module:kwsMediaApi/core~MediaPad.getMediaElementCallback} [callback]
 *
 * @return {module:kwsMediaApi/core~MediaPad}
 *  The own media object
 */
MediaPad.prototype.getMediaElement = function(callback){
  return this.invoke('getMediaElement', callback);
};
/**
 * @callback MediaPad~getMediaElementCallback
 * @param {Error} error
 * @param {MediaElement} result
 *  the element
 */

/**
 * Obtains the type of media that this pad accepts
 *
 * @param {module:kwsMediaApi/core~MediaPad.getMediaTypeCallback} [callback]
 *
 * @return {module:kwsMediaApi/core~MediaPad}
 *  The own media object
 */
MediaPad.prototype.getMediaType = function(callback){
  return this.invoke('getMediaType', callback);
};
/**
 * @callback MediaPad~getMediaTypeCallback
 * @param {Error} error
 * @param {MediaType} result
 *  One of :rom:attr:`MediaType.AUDIO`, :rom:attr:`MediaType.DATA` or :rom:attr:`MediaType.VIDEO`
 */


/**
 * @type module:kwsMediaApi/core~MediaPad.constructorParams
 */
MediaPad.constructorParams = {};

/**
 * @type   module:kwsMediaApi/core~MediaPad.events
 * @extend module:kwsMediaApi~MediaObject.events
 */
MediaPad.events = [];
MediaPad.events.concat(MediaObject.events);


module.exports = MediaPad;


MediaPad.check = function(key, value)
{
  if(!(value instanceof MediaPad))
    throw SyntaxError(key+' param should be a MediaPad, not '+typeof value);
};

},{"../checkType":3,"./MediaObject":17,"inherits":68}],19:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var Promise = require('es6-promise').Promise;

var promiseCallback = require('../utils').promiseCallback;


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/core
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var MediaObject = require('./MediaObject');


/**
 * A pipeline is a container for a collection of :rom:cls:`MediaElements<MediaElement>` and :rom:cls:`MediaMixers<MediaMixer>`. It offers the methods needed to control the creation and connection of elements inside a certain pipeline.
 *
 * @class   module:kwsMediaApi/core~MediaPipeline
 * @extends module:kwsMediaApi~MediaObject
 */

/**
 * Create a :rom:cls:`MediaPipeline`
 *
 * @constructor
 *
 * @param {string} id
 */
function MediaPipeline(id)
{
  MediaObject.call(this, id);
};
inherits(MediaPipeline, MediaObject);


/**
 * Create a new instance of a {module:kwsMediaApi/core~MediaObject} attached to this {module:kwsMediaApi/core~MediaPipeline}
 *
 * @param {external:string} type - Type of the {module:kwsMediaApi/core~MediaObject}
 * @param {external:string[]} [params]
 * @callback {module:kwsMediaApi/core~MediaPipeline~createCallback} callback
 *
 * @return {module:kwsMediaApi/core~MediaPipeline} The pipeline itself
 */
MediaPipeline.prototype.create = function(type, params, callback){
  var self = this;

  // Fix optional parameters
  if(params instanceof Function){
    if(callback)
      throw new SyntaxError("Nothing can be defined after the callback");

    callback = params;
    params = undefined;
  };

  params = params || {};

  var promise = new Promise(function(resolve, reject)
  {
    params.mediaPipeline = self;

    self.emit('_create', type, params, function(error, result)
    {
      if(error) return reject(error);

      resolve(result);
    });
  });

  promiseCallback(promise, callback);

  return promise;
};
/**
 * @callback module:kwsMediaApi/core~MediaPipeline~createCallback
 * @param {Error} error
 * @param {module:kwsMediaApi/core~MediaElement} result
 *  The created MediaElement
 */


/**
 * @type module:kwsMediaApi/core~MediaPipeline.constructorParams
 */
MediaPipeline.constructorParams = {};

/**
 * @type   module:kwsMediaApi/core~MediaPipeline.events
 * @extend module:kwsMediaApi~MediaObject.events
 */
MediaPipeline.events = [];
MediaPipeline.events.concat(MediaObject.events);


module.exports = MediaPipeline;


MediaPipeline.check = function(key, value)
{
  if(!(value instanceof MediaPipeline))
    throw SyntaxError(key+' param should be a MediaPipeline, not '+typeof value);
};

},{"../utils":48,"./MediaObject":17,"es6-promise":50,"inherits":68}],20:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var checkType = require('../checkType');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/core
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var MediaPad = require('./MediaPad');


/**
 * Special type of pad, used by a :rom:cls:`MediaElement` to receive a media stream.
 *
 * @abstract
 * @class   module:kwsMediaApi/core~MediaSink
 * @extends module:kwsMediaApi~MediaPad
 */

/**
 * @constructor
 *
 * @param {string} id
 */
function MediaSink(id)
{
  MediaPad.call(this, id);
};
inherits(MediaSink, MediaPad);


/**
 * Disconnects the current sink from the referred :rom:cls:`MediaSource`
 *
 * @param {MediaSource} src
 *  The source to disconnect
 *
 * @param {module:kwsMediaApi/core~MediaSink.disconnectCallback} [callback]
 *
 * @return {module:kwsMediaApi/core~MediaSink}
 *  The own media object
 */
MediaSink.prototype.disconnect = function(src, callback){
  checkType('MediaSource', 'src', src, {required: true});

  var params = {
    src: src,
  };

  return this.invoke('disconnect', params, callback);
};
/**
 * @callback MediaSink~disconnectCallback
 * @param {Error} error
 */

/**
 * Gets the :rom:cls:`MediaSource` that is connected to this sink.
 *
 * @param {module:kwsMediaApi/core~MediaSink.getConnectedSrcCallback} [callback]
 *
 * @return {module:kwsMediaApi/core~MediaSink}
 *  The own media object
 */
MediaSink.prototype.getConnectedSrc = function(callback){
  return this.invoke('getConnectedSrc', callback);
};
/**
 * @callback MediaSink~getConnectedSrcCallback
 * @param {Error} error
 * @param {MediaSource} result
 *  The source connected to this sink
 */


/**
 * @type module:kwsMediaApi/core~MediaSink.constructorParams
 */
MediaSink.constructorParams = {};

/**
 * @type   module:kwsMediaApi/core~MediaSink.events
 * @extend module:kwsMediaApi~MediaPad.events
 */
MediaSink.events = [];
MediaSink.events.concat(MediaPad.events);


module.exports = MediaSink;


MediaSink.check = function(key, value)
{
  if(!(value instanceof MediaSink))
    throw SyntaxError(key+' param should be a MediaSink, not '+typeof value);
};

},{"../checkType":3,"./MediaPad":18,"inherits":68}],21:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var checkType = require('../checkType');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/core
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var MediaPad = require('./MediaPad');


/**
 * Special type of pad, used by a media element to generate a media stream.
 *
 * @abstract
 * @class   module:kwsMediaApi/core~MediaSource
 * @extends module:kwsMediaApi~MediaPad
 */

/**
 * @constructor
 *
 * @param {string} id
 */
function MediaSource(id)
{
  MediaPad.call(this, id);
};
inherits(MediaSource, MediaPad);


/**
 * Connects the current source with a :rom:cls:`MediaSink`
 *
 * @param {MediaSink} sink
 *  The sink to connect this source
 *
 * @param {module:kwsMediaApi/core~MediaSource.connectCallback} [callback]
 *
 * @return {module:kwsMediaApi/core~MediaSource}
 *  The own media object
 */
MediaSource.prototype.connect = function(sink, callback){
  checkType('MediaSink', 'sink', sink, {required: true});

  var params = {
    sink: sink,
  };

  return this.invoke('connect', params, callback);
};
/**
 * @callback MediaSource~connectCallback
 * @param {Error} error
 */

/**
 * Gets all the :rom:cls:`MediaSinks<MediaSink>` to which this source is connected
 *
 * @param {module:kwsMediaApi/core~MediaSource.getConnectedSinksCallback} [callback]
 *
 * @return {module:kwsMediaApi/core~MediaSource}
 *  The own media object
 */
MediaSource.prototype.getConnectedSinks = function(callback){
  return this.invoke('getConnectedSinks', callback);
};
/**
 * @callback MediaSource~getConnectedSinksCallback
 * @param {Error} error
 * @param {MediaSink} result
 *  the list of sinks that the source is connected to
 */


/**
 * Disconnect this source pad from the specified sink pad
 *
 * @public
 *
 * @param {...module:kwsMediaApi/core~MediaSink} sink - Sink to be disconnected
 * @callback {module:kwsMediaApi/core~MediaSource~disconnectCallback} callback
 *
 * @return {module:kwsMediaApi/core~MediaSource} The own {module:kwsMediaApi/core~MediaSource}
 */
MediaSource.prototype.disconnect = function(sink, callback)
{
  checkType('MediaSink', 'sink', sink, {required: true});

  var params =
  {
    sink: sink
  };

  return this.invoke('disconnect', params, callback);
};
/**
 * @callback module:kwsMediaApi/core~MediaSource~disconnectCallback
 * @param {Error} error
 */


/**
 * @type module:kwsMediaApi/core~MediaSource.constructorParams
 */
MediaSource.constructorParams = {};

/**
 * @type   module:kwsMediaApi/core~MediaSource.events
 * @extend module:kwsMediaApi~MediaPad.events
 */
MediaSource.events = [];
MediaSource.events.concat(MediaPad.events);


module.exports = MediaSource;


MediaSource.check = function(key, value)
{
  if(!(value instanceof MediaSource))
    throw SyntaxError(key+' param should be a MediaSource, not '+typeof value);
};

},{"../checkType":3,"./MediaPad":18,"inherits":68}],22:[function(require,module,exports){
/*
 * (C) Copyright 2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/core
 *
 * @copyright 2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var HubPort = require('./HubPort');
var MediaPipeline = require('./MediaPipeline');


exports.HubPort = HubPort;
exports.MediaPipeline = MediaPipeline;

},{"./HubPort":15,"./MediaPipeline":19}],23:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var checkType = require('../checkType');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/endpoints
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var SessionEndpoint = require('./SessionEndpoint');


/**
 * Endpoint that enables Kurento to work as an HTTP server, allowing peer HTTP clients to access media.
 *
 * @abstract
 * @class   module:kwsMediaApi/endpoints~HttpEndpoint
 * @extends module:kwsMediaApi~SessionEndpoint
 */

/**
 * @constructor
 *
 * @param {string} id
 */
function HttpEndpoint(id)
{
  SessionEndpoint.call(this, id);
};
inherits(HttpEndpoint, SessionEndpoint);


/**
 * Obtains the URL associated to this endpoint
 *
 * @param {module:kwsMediaApi/endpoints~HttpEndpoint.getUrlCallback} [callback]
 *
 * @return {module:kwsMediaApi/endpoints~HttpEndpoint}
 *  The own media object
 */
HttpEndpoint.prototype.getUrl = function(callback){
  return this.invoke('getUrl', callback);
};
/**
 * @callback HttpEndpoint~getUrlCallback
 * @param {Error} error
 * @param {String} result
 *  The url as a String
 */


/**
 * @type module:kwsMediaApi/endpoints~HttpEndpoint.constructorParams
 */
HttpEndpoint.constructorParams = {};

/**
 * @type   module:kwsMediaApi/endpoints~HttpEndpoint.events
 * @extend module:kwsMediaApi~SessionEndpoint.events
 */
HttpEndpoint.events = [];
HttpEndpoint.events.concat(SessionEndpoint.events);


module.exports = HttpEndpoint;


HttpEndpoint.check = function(key, value)
{
  if(!(value instanceof HttpEndpoint))
    throw SyntaxError(key+' param should be a HttpEndpoint, not '+typeof value);
};

},{"../checkType":3,"./SessionEndpoint":30,"inherits":68}],24:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/endpoints
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var HttpEndpoint = require('./HttpEndpoint');


/**
 * An ``HttpGetEndpoint`` contains SOURCE pads for AUDIO and VIDEO, delivering media using HTML5 pseudo-streaming mechanism.

   This type of endpoint provide unidirectional communications. Its :rom:cls:`MediaSink` is associated with the HTTP GET method
 *
 * @class   module:kwsMediaApi/endpoints~HttpGetEndpoint
 * @extends module:kwsMediaApi~HttpEndpoint
 */

/**
 * Builder for the :rom:cls:`HttpGetEndpoint`.
 *
 * @constructor
 *
 * @param {string} id
 */
function HttpGetEndpoint(id)
{
  HttpEndpoint.call(this, id);
};
inherits(HttpGetEndpoint, HttpEndpoint);


/**
 * @type module:kwsMediaApi/endpoints~HttpGetEndpoint.constructorParams
 *
 * @property {int} [disconnectionTimeout]
 *  disconnection timeout in seconds.

This is the time that an http endpoint will wait for a reconnection, in case an HTTP connection is lost.
 *
 * @property {MediaPipeline} mediaPipeline
 *  the :rom:cls:`MediaPipeline` to which the endpoint belongs
 *
 * @property {MediaProfileSpecType} [mediaProfile]
 *  the :rom:enum:`MediaProfileSpecType` (WEBM, MP4...) for the endpoint
 *
 * @property {boolean} [terminateOnEOS]
 *  raise a :rom:evnt:`MediaSessionTerminated` event when the associated player raises a :rom:evnt:`EndOfStream`, and thus terminate the media session
 */
HttpGetEndpoint.constructorParams = {
  disconnectionTimeout: {
    type: 'int',
  },

  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },

  mediaProfile: {
    type: 'MediaProfileSpecType',
  },

  terminateOnEOS: {
    type: 'boolean',
  },
};

/**
 * @type   module:kwsMediaApi/endpoints~HttpGetEndpoint.events
 * @extend module:kwsMediaApi~HttpEndpoint.events
 */
HttpGetEndpoint.events = [];
HttpGetEndpoint.events.concat(HttpEndpoint.events);


module.exports = HttpGetEndpoint;


HttpGetEndpoint.check = function(key, value)
{
  if(!(value instanceof HttpGetEndpoint))
    throw SyntaxError(key+' param should be a HttpGetEndpoint, not '+typeof value);
};

},{"./HttpEndpoint":23,"inherits":68}],25:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/endpoints
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var HttpEndpoint = require('./HttpEndpoint');


/**
 * An :rom:cls:`HttpPostEndpoint` contains SINK pads for AUDIO and VIDEO, which provide access to an HTTP file upload function

   This type of endpoint provide unidirectional communications. Its :rom:cls:`MediaSources <MediaSource>` are accessed through the :term:`HTTP` POST method.
 *
 * @class   module:kwsMediaApi/endpoints~HttpPostEndpoint
 * @extends module:kwsMediaApi~HttpEndpoint
 */

/**
 * Builder for the :rom:cls:`HttpPostEndpoint`.
 *
 * @constructor
 *
 * @param {string} id
 */
function HttpPostEndpoint(id)
{
  HttpEndpoint.call(this, id);
};
inherits(HttpPostEndpoint, HttpEndpoint);


/**
 * @type module:kwsMediaApi/endpoints~HttpPostEndpoint.constructorParams
 *
 * @property {int} [disconnectionTimeout]
 *  This is the time that an http endpoint will wait for a reconnection, in case an HTTP connection is lost.
 *
 * @property {MediaPipeline} mediaPipeline
 *  the :rom:cls:`MediaPipeline` to which the endpoint belongs
 *
 * @property {boolean} [useEncodedMedia]
 *  configures the endpoint to use encoded media instead of raw media. If the parameter is not set then the element uses raw media. Changing this parameter could affect in a severe way to stability because key frames lost will not be generated. Changing the media type does not affect to the result except in the performance (just in the case where original media and target media are the same) and in the problem with the key frames. We strongly recommended not to use this parameter because correct behaviour is not guarantied.
 */
HttpPostEndpoint.constructorParams = {
  disconnectionTimeout: {
    type: 'int',
  },

  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },

  useEncodedMedia: {
    type: 'boolean',
  },
};

/**
 * @type   module:kwsMediaApi/endpoints~HttpPostEndpoint.events
 * @extend module:kwsMediaApi~HttpEndpoint.events
 */
HttpPostEndpoint.events = ['EndOfStream'];
HttpPostEndpoint.events.concat(HttpEndpoint.events);


module.exports = HttpPostEndpoint;


HttpPostEndpoint.check = function(key, value)
{
  if(!(value instanceof HttpPostEndpoint))
    throw SyntaxError(key+' param should be a HttpPostEndpoint, not '+typeof value);
};

},{"./HttpEndpoint":23,"inherits":68}],26:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var checkType = require('../checkType');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/endpoints
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var UriEndpoint = require('./UriEndpoint');


/**
 * Retrieves content from seekable sources in reliable
mode (does not discard media information) and inject 
them into :term:`KMS`. It
contains one :rom:cls:`MediaSource` for each media type detected.
 *
 * @class   module:kwsMediaApi/endpoints~PlayerEndpoint
 * @extends module:kwsMediaApi~UriEndpoint
 */

/**
 * Create a PlayerEndpoint
 *
 * @constructor
 *
 * @param {string} id
 */
function PlayerEndpoint(id)
{
  UriEndpoint.call(this, id);
};
inherits(PlayerEndpoint, UriEndpoint);


/**
 * Starts to send data to the endpoint :rom:cls:`MediaSource`
 *
 * @param {module:kwsMediaApi/endpoints~PlayerEndpoint.playCallback} [callback]
 *
 * @return {module:kwsMediaApi/endpoints~PlayerEndpoint}
 *  The own media object
 */
PlayerEndpoint.prototype.play = function(callback){
  return this.invoke('play', callback);
};
/**
 * @callback PlayerEndpoint~playCallback
 * @param {Error} error
 */


/**
 * @type module:kwsMediaApi/endpoints~PlayerEndpoint.constructorParams
 *
 * @property {MediaPipeline} mediaPipeline
 *  The :rom:cls:`MediaPipeline` this PlayerEndpoint belongs to.
 *
 * @property {String} uri
 *  URI that will be played
 *
 * @property {boolean} [useEncodedMedia]
 *  use encoded instead of raw media. If the parameter is false then the
element uses raw media. Changing this parameter can affect stability
severely, as lost key frames lost will not be regenerated. Changing the media type does not
affect to the result except in the performance (just in the case where
original media and target media are the same) and in the problem with the
key frames. We strongly recommended not to use this parameter because
correct behaviour is not guarantied.
 */
PlayerEndpoint.constructorParams = {
  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },

  uri: {
    type: 'String',
    required: true
  },

  useEncodedMedia: {
    type: 'boolean',
  },
};

/**
 * @type   module:kwsMediaApi/endpoints~PlayerEndpoint.events
 * @extend module:kwsMediaApi~UriEndpoint.events
 */
PlayerEndpoint.events = ['EndOfStream'];
PlayerEndpoint.events.concat(UriEndpoint.events);


module.exports = PlayerEndpoint;


PlayerEndpoint.check = function(key, value)
{
  if(!(value instanceof PlayerEndpoint))
    throw SyntaxError(key+' param should be a PlayerEndpoint, not '+typeof value);
};

},{"../checkType":3,"./UriEndpoint":31,"inherits":68}],27:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var checkType = require('../checkType');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/endpoints
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var UriEndpoint = require('./UriEndpoint');


/**
 * Provides function to store contents in reliable mode (doesn't discard data). It contains :rom:cls:`MediaSink` pads for audio and video.
 *
 * @class   module:kwsMediaApi/endpoints~RecorderEndpoint
 * @extends module:kwsMediaApi~UriEndpoint
 */

/**
 * 
 *
 * @constructor
 *
 * @param {string} id
 */
function RecorderEndpoint(id)
{
  UriEndpoint.call(this, id);
};
inherits(RecorderEndpoint, UriEndpoint);


/**
 * Starts storing media received through the :rom:cls:`MediaSink` pad
 *
 * @param {module:kwsMediaApi/endpoints~RecorderEndpoint.recordCallback} [callback]
 *
 * @return {module:kwsMediaApi/endpoints~RecorderEndpoint}
 *  The own media object
 */
RecorderEndpoint.prototype.record = function(callback){
  return this.invoke('record', callback);
};
/**
 * @callback RecorderEndpoint~recordCallback
 * @param {Error} error
 */


/**
 * @type module:kwsMediaApi/endpoints~RecorderEndpoint.constructorParams
 *
 * @property {MediaPipeline} mediaPipeline
 *  the :rom:cls:`MediaPipeline` to which the endpoint belongs
 *
 * @property {MediaProfileSpecType} [mediaProfile]
 *  Choose either a :rom:attr:`MediaProfileSpecType.WEBM` or a :rom:attr:`MediaProfileSpecType.MP4` profile for recording
 *
 * @property {boolean} [stopOnEndOfStream]
 *  Forces the recorder endpoint to finish processing data when an :term:`EOS` is detected in the stream
 *
 * @property {String} uri
 *  URI where the recording will be stored
 */
RecorderEndpoint.constructorParams = {
  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },

  mediaProfile: {
    type: 'MediaProfileSpecType',
  },

  stopOnEndOfStream: {
    type: 'boolean',
  },

  uri: {
    type: 'String',
    required: true
  },
};

/**
 * @type   module:kwsMediaApi/endpoints~RecorderEndpoint.events
 * @extend module:kwsMediaApi~UriEndpoint.events
 */
RecorderEndpoint.events = [];
RecorderEndpoint.events.concat(UriEndpoint.events);


module.exports = RecorderEndpoint;


RecorderEndpoint.check = function(key, value)
{
  if(!(value instanceof RecorderEndpoint))
    throw SyntaxError(key+' param should be a RecorderEndpoint, not '+typeof value);
};

},{"../checkType":3,"./UriEndpoint":31,"inherits":68}],28:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/endpoints
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var SdpEndpoint = require('./SdpEndpoint');


/**
 * Endpoint that provides bidirectional content delivery capabilities with remote networked peers through RTP protocol. An :rom:cls:`RtpEndpoint` contains paired sink and source :rom:cls:`MediaPad` for audio and video.
 *
 * @class   module:kwsMediaApi/endpoints~RtpEndpoint
 * @extends module:kwsMediaApi~SdpEndpoint
 */

/**
 * Builder for the :rom:cls:`RtpEndpoint`
 *
 * @constructor
 *
 * @param {string} id
 */
function RtpEndpoint(id)
{
  SdpEndpoint.call(this, id);
};
inherits(RtpEndpoint, SdpEndpoint);


/**
 * @type module:kwsMediaApi/endpoints~RtpEndpoint.constructorParams
 *
 * @property {MediaPipeline} mediaPipeline
 *  the :rom:cls:`MediaPipeline` to which the endpoint belongs
 */
RtpEndpoint.constructorParams = {
  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },
};

/**
 * @type   module:kwsMediaApi/endpoints~RtpEndpoint.events
 * @extend module:kwsMediaApi~SdpEndpoint.events
 */
RtpEndpoint.events = [];
RtpEndpoint.events.concat(SdpEndpoint.events);


module.exports = RtpEndpoint;


RtpEndpoint.check = function(key, value)
{
  if(!(value instanceof RtpEndpoint))
    throw SyntaxError(key+' param should be a RtpEndpoint, not '+typeof value);
};

},{"./SdpEndpoint":29,"inherits":68}],29:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var checkType = require('../checkType');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/endpoints
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var SessionEndpoint = require('./SessionEndpoint');


/**
 * Implements an SDP negotiation endpoint able to generate and process offers/responses and that configures resources according to negotiated Session Description
 *
 * @abstract
 * @class   module:kwsMediaApi/endpoints~SdpEndpoint
 * @extends module:kwsMediaApi~SessionEndpoint
 */

/**
 * @constructor
 *
 * @param {string} id
 */
function SdpEndpoint(id)
{
  SessionEndpoint.call(this, id);
};
inherits(SdpEndpoint, SessionEndpoint);


/**
 * Request a SessionSpec offer.

   This can be used to initiate a connection.
 *
 * @param {module:kwsMediaApi/endpoints~SdpEndpoint.generateOfferCallback} [callback]
 *
 * @return {module:kwsMediaApi/endpoints~SdpEndpoint}
 *  The own media object
 */
SdpEndpoint.prototype.generateOffer = function(callback){
  return this.invoke('generateOffer', callback);
};
/**
 * @callback SdpEndpoint~generateOfferCallback
 * @param {Error} error
 * @param {String} result
 *  The SDP offer.
 */

/**
 * This method gives access to the SessionSpec offered by this NetworkConnection.

.. note:: This method returns the local MediaSpec, negotiated or not. If no offer has been generated yet, it returns null. It an offer has been generated it returns the offer and if an answer has been processed it returns the negotiated local SessionSpec.
 *
 * @param {module:kwsMediaApi/endpoints~SdpEndpoint.getLocalSessionDescriptorCallback} [callback]
 *
 * @return {module:kwsMediaApi/endpoints~SdpEndpoint}
 *  The own media object
 */
SdpEndpoint.prototype.getLocalSessionDescriptor = function(callback){
  return this.invoke('getLocalSessionDescriptor', callback);
};
/**
 * @callback SdpEndpoint~getLocalSessionDescriptorCallback
 * @param {Error} error
 * @param {String} result
 *  The last agreed SessionSpec
 */

/**
 * This method gives access to the remote session description.

.. note:: This method returns the media previously agreed after a complete offer-answer exchange. If no media has been agreed yet, it returns null.
 *
 * @param {module:kwsMediaApi/endpoints~SdpEndpoint.getRemoteSessionDescriptorCallback} [callback]
 *
 * @return {module:kwsMediaApi/endpoints~SdpEndpoint}
 *  The own media object
 */
SdpEndpoint.prototype.getRemoteSessionDescriptor = function(callback){
  return this.invoke('getRemoteSessionDescriptor', callback);
};
/**
 * @callback SdpEndpoint~getRemoteSessionDescriptorCallback
 * @param {Error} error
 * @param {String} result
 *  The last agreed User Agent session description
 */

/**
 * Request the NetworkConnection to process the given SessionSpec answer (from the remote User Agent).
 *
 * @param {String} answer
 *  SessionSpec answer from the remote User Agent
 *
 * @param {module:kwsMediaApi/endpoints~SdpEndpoint.processAnswerCallback} [callback]
 *
 * @return {module:kwsMediaApi/endpoints~SdpEndpoint}
 *  The own media object
 */
SdpEndpoint.prototype.processAnswer = function(answer, callback){
  checkType('String', 'answer', answer, {required: true});

  var params = {
    answer: answer,
  };

  return this.invoke('processAnswer', params, callback);
};
/**
 * @callback SdpEndpoint~processAnswerCallback
 * @param {Error} error
 * @param {String} result
 *  Updated SDP offer, based on the answer received.
 */

/**
 * Request the NetworkConnection to process the given SessionSpec offer (from the remote User Agent)
 *
 * @param {String} offer
 *  SessionSpec offer from the remote User Agent
 *
 * @param {module:kwsMediaApi/endpoints~SdpEndpoint.processOfferCallback} [callback]
 *
 * @return {module:kwsMediaApi/endpoints~SdpEndpoint}
 *  The own media object
 */
SdpEndpoint.prototype.processOffer = function(offer, callback){
  checkType('String', 'offer', offer, {required: true});

  var params = {
    offer: offer,
  };

  return this.invoke('processOffer', params, callback);
};
/**
 * @callback SdpEndpoint~processOfferCallback
 * @param {Error} error
 * @param {String} result
 *  The chosen configuration from the ones stated in the SDP offer
 */


/**
 * @type module:kwsMediaApi/endpoints~SdpEndpoint.constructorParams
 */
SdpEndpoint.constructorParams = {};

/**
 * @type   module:kwsMediaApi/endpoints~SdpEndpoint.events
 * @extend module:kwsMediaApi~SessionEndpoint.events
 */
SdpEndpoint.events = [];
SdpEndpoint.events.concat(SessionEndpoint.events);


module.exports = SdpEndpoint;


SdpEndpoint.check = function(key, value)
{
  if(!(value instanceof SdpEndpoint))
    throw SyntaxError(key+' param should be a SdpEndpoint, not '+typeof value);
};

},{"../checkType":3,"./SessionEndpoint":30,"inherits":68}],30:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/endpoints
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var Endpoint = require('../core/Endpoint');


/**
 * Session based endpoint. A session is considered to be started when the media exchange starts. On the other hand, sessions terminate when a timeout, defined by the developer, takes place after the connection is lost.
 *
 * @abstract
 * @class   module:kwsMediaApi/endpoints~SessionEndpoint
 * @extends module:kwsMediaApi~Endpoint
 */

/**
 * @constructor
 *
 * @param {string} id
 */
function SessionEndpoint(id)
{
  Endpoint.call(this, id);
};
inherits(SessionEndpoint, Endpoint);


/**
 * @type module:kwsMediaApi/endpoints~SessionEndpoint.constructorParams
 */
SessionEndpoint.constructorParams = {};

/**
 * @type   module:kwsMediaApi/endpoints~SessionEndpoint.events
 * @extend module:kwsMediaApi~Endpoint.events
 */
SessionEndpoint.events = ['MediaSessionStarted', 'MediaSessionTerminated'];
SessionEndpoint.events.concat(Endpoint.events);


module.exports = SessionEndpoint;


SessionEndpoint.check = function(key, value)
{
  if(!(value instanceof SessionEndpoint))
    throw SyntaxError(key+' param should be a SessionEndpoint, not '+typeof value);
};

},{"../core/Endpoint":12,"inherits":68}],31:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var checkType = require('../checkType');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/endpoints
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var Endpoint = require('../core/Endpoint');


/**
 * Interface for endpoints the require a URI to work. An example of this, would be a :rom:cls:`PlayerEndpoint` whose URI property could be used to locate a file to stream through its :rom:cls:`MediaSource`
 *
 * @abstract
 * @class   module:kwsMediaApi/endpoints~UriEndpoint
 * @extends module:kwsMediaApi~Endpoint
 */

/**
 * @constructor
 *
 * @param {string} id
 */
function UriEndpoint(id)
{
  Endpoint.call(this, id);
};
inherits(UriEndpoint, Endpoint);


/**
 * Returns the uri for this endpoint.
 *
 * @param {module:kwsMediaApi/endpoints~UriEndpoint.getUriCallback} [callback]
 *
 * @return {module:kwsMediaApi/endpoints~UriEndpoint}
 *  The own media object
 */
UriEndpoint.prototype.getUri = function(callback){
  return this.invoke('getUri', callback);
};
/**
 * @callback UriEndpoint~getUriCallback
 * @param {Error} error
 * @param {String} result
 *  the uri as a String
 */

/**
 * Pauses the feed
 *
 * @param {module:kwsMediaApi/endpoints~UriEndpoint.pauseCallback} [callback]
 *
 * @return {module:kwsMediaApi/endpoints~UriEndpoint}
 *  The own media object
 */
UriEndpoint.prototype.pause = function(callback){
  return this.invoke('pause', callback);
};
/**
 * @callback UriEndpoint~pauseCallback
 * @param {Error} error
 */

/**
 * Stops the feed
 *
 * @param {module:kwsMediaApi/endpoints~UriEndpoint.stopCallback} [callback]
 *
 * @return {module:kwsMediaApi/endpoints~UriEndpoint}
 *  The own media object
 */
UriEndpoint.prototype.stop = function(callback){
  return this.invoke('stop', callback);
};
/**
 * @callback UriEndpoint~stopCallback
 * @param {Error} error
 */


/**
 * @type module:kwsMediaApi/endpoints~UriEndpoint.constructorParams
 */
UriEndpoint.constructorParams = {};

/**
 * @type   module:kwsMediaApi/endpoints~UriEndpoint.events
 * @extend module:kwsMediaApi~Endpoint.events
 */
UriEndpoint.events = [];
UriEndpoint.events.concat(Endpoint.events);


module.exports = UriEndpoint;


UriEndpoint.check = function(key, value)
{
  if(!(value instanceof UriEndpoint))
    throw SyntaxError(key+' param should be a UriEndpoint, not '+typeof value);
};

},{"../checkType":3,"../core/Endpoint":12,"inherits":68}],32:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/endpoints
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var SdpEndpoint = require('./SdpEndpoint');


/**
 * WebRtcEndpoint interface. This type of ``Endpoint`` offers media streaming using WebRTC.
 *
 * @class   module:kwsMediaApi/endpoints~WebRtcEndpoint
 * @extends module:kwsMediaApi~SdpEndpoint
 */

/**
 * Builder for the :rom:cls:`WebRtcEndpoint`
 *
 * @constructor
 *
 * @param {string} id
 */
function WebRtcEndpoint(id)
{
  SdpEndpoint.call(this, id);
};
inherits(WebRtcEndpoint, SdpEndpoint);


/**
 * @type module:kwsMediaApi/endpoints~WebRtcEndpoint.constructorParams
 *
 * @property {MediaPipeline} mediaPipeline
 *  the :rom:cls:`MediaPipeline` to which the endpoint belongs
 */
WebRtcEndpoint.constructorParams = {
  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },
};

/**
 * @type   module:kwsMediaApi/endpoints~WebRtcEndpoint.events
 * @extend module:kwsMediaApi~SdpEndpoint.events
 */
WebRtcEndpoint.events = [];
WebRtcEndpoint.events.concat(SdpEndpoint.events);


module.exports = WebRtcEndpoint;


WebRtcEndpoint.check = function(key, value)
{
  if(!(value instanceof WebRtcEndpoint))
    throw SyntaxError(key+' param should be a WebRtcEndpoint, not '+typeof value);
};

},{"./SdpEndpoint":29,"inherits":68}],33:[function(require,module,exports){
/*
 * (C) Copyright 2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/endpoints
 *
 * @copyright 2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var HttpGetEndpoint = require('./HttpGetEndpoint');
var HttpPostEndpoint = require('./HttpPostEndpoint');
var PlayerEndpoint = require('./PlayerEndpoint');
var RecorderEndpoint = require('./RecorderEndpoint');
var RtpEndpoint = require('./RtpEndpoint');
var WebRtcEndpoint = require('./WebRtcEndpoint');


exports.HttpGetEndpoint = HttpGetEndpoint;
exports.HttpPostEndpoint = HttpPostEndpoint;
exports.PlayerEndpoint = PlayerEndpoint;
exports.RecorderEndpoint = RecorderEndpoint;
exports.RtpEndpoint = RtpEndpoint;
exports.WebRtcEndpoint = WebRtcEndpoint;

},{"./HttpGetEndpoint":24,"./HttpPostEndpoint":25,"./PlayerEndpoint":26,"./RecorderEndpoint":27,"./RtpEndpoint":28,"./WebRtcEndpoint":32}],34:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var checkType = require('../checkType');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/filters
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var Filter = require('../core/Filter');


/**
 * ChromaFilter interface. This type of :rom:cls:`Filter` makes transparent a colour
range in the top layer, revealing another image behind
 *
 * @class   module:kwsMediaApi/filters~ChromaFilter
 * @extends module:kwsMediaApi~Filter
 */

/**
 * Create a :rom:cls:`ChromaFilter`
 *
 * @constructor
 *
 * @param {string} id
 */
function ChromaFilter(id)
{
  Filter.call(this, id);
};
inherits(ChromaFilter, Filter);


/**
 * Sets the image to show on the detected chroma surface.
 *
 * @param {String} uri
 *  URI where the image is located
 *
 * @param {module:kwsMediaApi/filters~ChromaFilter.setBackgroundCallback} [callback]
 *
 * @return {module:kwsMediaApi/filters~ChromaFilter}
 *  The own media object
 */
ChromaFilter.prototype.setBackground = function(uri, callback){
  checkType('String', 'uri', uri, {required: true});

  var params = {
    uri: uri,
  };

  return this.invoke('setBackground', params, callback);
};
/**
 * @callback ChromaFilter~setBackgroundCallback
 * @param {Error} error
 */

/**
 * Clears the image used to be shown behind the chroma surface.
 *
 * @param {module:kwsMediaApi/filters~ChromaFilter.unsetBackgroundCallback} [callback]
 *
 * @return {module:kwsMediaApi/filters~ChromaFilter}
 *  The own media object
 */
ChromaFilter.prototype.unsetBackground = function(callback){
  return this.invoke('unsetBackground', callback);
};
/**
 * @callback ChromaFilter~unsetBackgroundCallback
 * @param {Error} error
 */


/**
 * @type module:kwsMediaApi/filters~ChromaFilter.constructorParams
 *
 * @property {String} [backgroundImage]
 *  url of image to be used to replace the detected background
 *
 * @property {MediaPipeline} mediaPipeline
 *  the :rom:cls:`MediaPipeline` to which the filter belongs
 *
 * @property {WindowParam} window
 *  Window of replacement for the :rom:cls:`ChromaFilter`
 */
ChromaFilter.constructorParams = {
  backgroundImage: {
    type: 'String',
  },

  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },

  window: {
    type: 'WindowParam',
    required: true
  },
};

/**
 * @type   module:kwsMediaApi/filters~ChromaFilter.events
 * @extend module:kwsMediaApi~Filter.events
 */
ChromaFilter.events = [];
ChromaFilter.events.concat(Filter.events);


module.exports = ChromaFilter;


ChromaFilter.check = function(key, value)
{
  if(!(value instanceof ChromaFilter))
    throw SyntaxError(key+' param should be a ChromaFilter, not '+typeof value);
};

},{"../checkType":3,"../core/Filter":13,"inherits":68}],35:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/filters
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var Filter = require('../core/Filter');


/**
 * Filter that detects people agglomeration in video streams
 *
 * @class   module:kwsMediaApi/filters~CrowdDetectorFilter
 * @extends module:kwsMediaApi~Filter
 */

/**
 * Create a :rom:cls:`CrowdDetectorFilter`
 *
 * @constructor
 *
 * @param {string} id
 */
function CrowdDetectorFilter(id)
{
  Filter.call(this, id);
};
inherits(CrowdDetectorFilter, Filter);


/**
 * @type module:kwsMediaApi/filters~CrowdDetectorFilter.constructorParams
 *
 * @property {MediaPipeline} mediaPipeline
 *  the :rom:cls:`MediaPipeline` to which the filter belongs
 *
 * @property {RegionOfInterest} rois
 *  Regions of interest for the filter
 */
CrowdDetectorFilter.constructorParams = {
  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },

  rois: {
    type: 'RegionOfInterest',
    isList: true,
    required: true
  },
};

/**
 * @type   module:kwsMediaApi/filters~CrowdDetectorFilter.events
 * @extend module:kwsMediaApi~Filter.events
 */
CrowdDetectorFilter.events = ['CrowdDetectorDirection', 'CrowdDetectorFluidity', 'CrowdDetectorOccupancy'];
CrowdDetectorFilter.events.concat(Filter.events);


module.exports = CrowdDetectorFilter;


CrowdDetectorFilter.check = function(key, value)
{
  if(!(value instanceof CrowdDetectorFilter))
    throw SyntaxError(key+' param should be a CrowdDetectorFilter, not '+typeof value);
};

},{"../core/Filter":13,"inherits":68}],36:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var checkType = require('../checkType');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/filters
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var Filter = require('../core/Filter');


/**
 * FaceOverlayFilter interface. This type of :rom:cls:`Filter` detects faces in a video feed. The face is then overlaid with an image.
 *
 * @class   module:kwsMediaApi/filters~FaceOverlayFilter
 * @extends module:kwsMediaApi~Filter
 */

/**
 * FaceOverlayFilter interface. This type of :rom:cls:`Filter` detects faces in a video feed. The face is then overlaid with an image.
 *
 * @constructor
 *
 * @param {string} id
 */
function FaceOverlayFilter(id)
{
  Filter.call(this, id);
};
inherits(FaceOverlayFilter, Filter);


/**
 * Sets the image to use as overlay on the detected faces.
 *
 * @param {String} uri
 *  URI where the image is located
 *
 * @param {float} offsetXPercent
 *  the offset applied to the image, from the X coordinate of the detected face upper right corner. A positive value indicates right displacement, while a negative value moves the overlaid image to the left. This offset is specified as a percentage of the face width.

For example, to cover the detected face with the overlaid image, the parameter has to be ``0.0``. Values of ``1.0`` or ``-1.0`` indicate that the image upper right corner will be at the face´s X coord, +- the face´s width.

.. note::

    The parameter name is misleading, the value is not a percent but a ratio
 *
 * @param {float} offsetYPercent
 *  the offset applied to the image, from the Y coordinate of the detected face upper right corner. A positive value indicates up displacement, while a negative value moves the overlaid image down. This offset is specified as a percentage of the face width.

For example, to cover the detected face with the overlaid image, the parameter has to be ``0.0``. Values of ``1.0`` or ``-1.0`` indicate that the image upper right corner will be at the face´s Y coord, +- the face´s width.

.. note::

    The parameter name is misleading, the value is not a percent but a ratio
 *
 * @param {float} widthPercent
 *  proportional width of the overlaid image, relative to the width of the detected face. A value of 1.0 implies that the overlaid image will have the same width as the detected face. Values greater than 1.0 are allowed, while negative values are forbidden.

.. note::

    The parameter name is misleading, the value is not a percent but a ratio
 *
 * @param {float} heightPercent
 *  proportional height of the overlaid image, relative to the height of the detected face. A value of 1.0 implies that the overlaid image will have the same height as the detected face. Values greater than 1.0 are allowed, while negative values are forbidden.

.. note::

    The parameter name is misleading, the value is not a percent but a ratio
 *
 * @param {module:kwsMediaApi/filters~FaceOverlayFilter.setOverlayedImageCallback} [callback]
 *
 * @return {module:kwsMediaApi/filters~FaceOverlayFilter}
 *  The own media object
 */
FaceOverlayFilter.prototype.setOverlayedImage = function(uri, offsetXPercent, offsetYPercent, widthPercent, heightPercent, callback){
  checkType('String', 'uri', uri, {required: true});
  checkType('float', 'offsetXPercent', offsetXPercent, {required: true});
  checkType('float', 'offsetYPercent', offsetYPercent, {required: true});
  checkType('float', 'widthPercent', widthPercent, {required: true});
  checkType('float', 'heightPercent', heightPercent, {required: true});

  var params = {
    uri: uri,
    offsetXPercent: offsetXPercent,
    offsetYPercent: offsetYPercent,
    widthPercent: widthPercent,
    heightPercent: heightPercent,
  };

  return this.invoke('setOverlayedImage', params, callback);
};
/**
 * @callback FaceOverlayFilter~setOverlayedImageCallback
 * @param {Error} error
 */

/**
 * Clear the image to be shown over each detected face. Stops overlaying the faces.
 *
 * @param {module:kwsMediaApi/filters~FaceOverlayFilter.unsetOverlayedImageCallback} [callback]
 *
 * @return {module:kwsMediaApi/filters~FaceOverlayFilter}
 *  The own media object
 */
FaceOverlayFilter.prototype.unsetOverlayedImage = function(callback){
  return this.invoke('unsetOverlayedImage', callback);
};
/**
 * @callback FaceOverlayFilter~unsetOverlayedImageCallback
 * @param {Error} error
 */


/**
 * @type module:kwsMediaApi/filters~FaceOverlayFilter.constructorParams
 *
 * @property {MediaPipeline} mediaPipeline
 *  pipeline to which this :rom:cls:`Filter` belons
 */
FaceOverlayFilter.constructorParams = {
  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },
};

/**
 * @type   module:kwsMediaApi/filters~FaceOverlayFilter.events
 * @extend module:kwsMediaApi~Filter.events
 */
FaceOverlayFilter.events = [];
FaceOverlayFilter.events.concat(Filter.events);


module.exports = FaceOverlayFilter;


FaceOverlayFilter.check = function(key, value)
{
  if(!(value instanceof FaceOverlayFilter))
    throw SyntaxError(key+' param should be a FaceOverlayFilter, not '+typeof value);
};

},{"../checkType":3,"../core/Filter":13,"inherits":68}],37:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/filters
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var Filter = require('../core/Filter');


/**
 * This is a generic filter interface, that creates GStreamer filters in the media server.
 *
 * @class   module:kwsMediaApi/filters~GStreamerFilter
 * @extends module:kwsMediaApi~Filter
 */

/**
 * Create a :rom:cls:`GStreamerFilter`
 *
 * @constructor
 *
 * @param {string} id
 */
function GStreamerFilter(id)
{
  Filter.call(this, id);
};
inherits(GStreamerFilter, Filter);


/**
 * @type module:kwsMediaApi/filters~GStreamerFilter.constructorParams
 *
 * @property {String} command
 *  command that would be used to instantiate the filter, as in `gst-launch <http://rpm.pbone.net/index.php3/stat/45/idpl/19531544/numer/1/nazwa/gst-launch-1.0>`__
 *
 * @property {MediaPipeline} mediaPipeline
 *  the :rom:cls:`MediaPipeline` to which the filter belongs
 */
GStreamerFilter.constructorParams = {
  command: {
    type: 'String',
    required: true
  },

  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },
};

/**
 * @type   module:kwsMediaApi/filters~GStreamerFilter.events
 * @extend module:kwsMediaApi~Filter.events
 */
GStreamerFilter.events = [];
GStreamerFilter.events.concat(Filter.events);


module.exports = GStreamerFilter;


GStreamerFilter.check = function(key, value)
{
  if(!(value instanceof GStreamerFilter))
    throw SyntaxError(key+' param should be a GStreamerFilter, not '+typeof value);
};

},{"../core/Filter":13,"inherits":68}],38:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/filters
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var Filter = require('../core/Filter');


/**
 * Filter that detects faces in a video feed. Those on the right half of the feed are overlaid with a pirate hat, and those on the left half are covered by a Darth Vader helmet. This is an example filter, intended to demonstrate how to integrate computer vision capabilities into the multimedia infrastructure.
 *
 * @class   module:kwsMediaApi/filters~JackVaderFilter
 * @extends module:kwsMediaApi~Filter
 */

/**
 * Create a new :rom:cls:`Filter`
 *
 * @constructor
 *
 * @param {string} id
 */
function JackVaderFilter(id)
{
  Filter.call(this, id);
};
inherits(JackVaderFilter, Filter);


/**
 * @type module:kwsMediaApi/filters~JackVaderFilter.constructorParams
 *
 * @property {MediaPipeline} mediaPipeline
 *  the :rom:cls:`MediaPipeline` to which the filter belongs
 */
JackVaderFilter.constructorParams = {
  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },
};

/**
 * @type   module:kwsMediaApi/filters~JackVaderFilter.events
 * @extend module:kwsMediaApi~Filter.events
 */
JackVaderFilter.events = [];
JackVaderFilter.events.concat(Filter.events);


module.exports = JackVaderFilter;


JackVaderFilter.check = function(key, value)
{
  if(!(value instanceof JackVaderFilter))
    throw SyntaxError(key+' param should be a JackVaderFilter, not '+typeof value);
};

},{"../core/Filter":13,"inherits":68}],39:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var checkType = require('../checkType');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/filters
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var Filter = require('../core/Filter');


/**
 * PlateDetectorFilter interface. This type of :rom:cls:`Endpoint` detects
vehicle plates in a video feed.
 *
 * @class   module:kwsMediaApi/filters~PlateDetectorFilter
 * @extends module:kwsMediaApi~Filter
 */

/**
 * Create a :rom:cls:`PlateDetectorFilter` for the given :rom:cls:`MediaPipeline`
 *
 * @constructor
 *
 * @param {string} id
 */
function PlateDetectorFilter(id)
{
  Filter.call(this, id);
};
inherits(PlateDetectorFilter, Filter);


/**
 * Configures the average width of the license plates in the image represented as an image percentage.
 *
 * @param {float} plateWidthPercentage
 *  average width of the license plates represented as an image percentage [0..1].
 *
 * @param {module:kwsMediaApi/filters~PlateDetectorFilter.setPlateWidthPercentageCallback} [callback]
 *
 * @return {module:kwsMediaApi/filters~PlateDetectorFilter}
 *  The own media object
 */
PlateDetectorFilter.prototype.setPlateWidthPercentage = function(plateWidthPercentage, callback){
  checkType('float', 'plateWidthPercentage', plateWidthPercentage, {required: true});

  var params = {
    plateWidthPercentage: plateWidthPercentage,
  };

  return this.invoke('setPlateWidthPercentage', params, callback);
};
/**
 * @callback PlateDetectorFilter~setPlateWidthPercentageCallback
 * @param {Error} error
 */


/**
 * @type module:kwsMediaApi/filters~PlateDetectorFilter.constructorParams
 *
 * @property {MediaPipeline} mediaPipeline
 *  the parent :rom:cls:`MediaPipeline` of this :rom:cls:`PlateDetectorFilter`
 */
PlateDetectorFilter.constructorParams = {
  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },
};

/**
 * @type   module:kwsMediaApi/filters~PlateDetectorFilter.events
 * @extend module:kwsMediaApi~Filter.events
 */
PlateDetectorFilter.events = ['PlateDetected'];
PlateDetectorFilter.events.concat(Filter.events);


module.exports = PlateDetectorFilter;


PlateDetectorFilter.check = function(key, value)
{
  if(!(value instanceof PlateDetectorFilter))
    throw SyntaxError(key+' param should be a PlateDetectorFilter, not '+typeof value);
};

},{"../checkType":3,"../core/Filter":13,"inherits":68}],40:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var checkType = require('../checkType');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/filters
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var Filter = require('../core/Filter');


/**
 * This type of :rom:cls:`Filter` detects UI pointers in a video feed.
 *
 * @class   module:kwsMediaApi/filters~PointerDetectorAdvFilter
 * @extends module:kwsMediaApi~Filter
 */

/**
 * Builder for the :rom:cls:`PointerDetectorAdvFilter`.
 *
 * @constructor
 *
 * @param {string} id
 */
function PointerDetectorAdvFilter(id)
{
  Filter.call(this, id);
};
inherits(PointerDetectorAdvFilter, Filter);


/**
 *  Adds a new detection window for the filter to detect pointers entering or exiting the window
 *
 * @param {PointerDetectorWindowMediaParam} window
 *  The window to be added
 *
 * @param {module:kwsMediaApi/filters~PointerDetectorAdvFilter.addWindowCallback} [callback]
 *
 * @return {module:kwsMediaApi/filters~PointerDetectorAdvFilter}
 *  The own media object
 */
PointerDetectorAdvFilter.prototype.addWindow = function(window, callback){
  checkType('PointerDetectorWindowMediaParam', 'window', window, {required: true});

  var params = {
    window: window,
  };

  return this.invoke('addWindow', params, callback);
};
/**
 * @callback PointerDetectorAdvFilter~addWindowCallback
 * @param {Error} error
 */

/**
 * Removes all pointer detector windows
 *
 * @param {module:kwsMediaApi/filters~PointerDetectorAdvFilter.clearWindowsCallback} [callback]
 *
 * @return {module:kwsMediaApi/filters~PointerDetectorAdvFilter}
 *  The own media object
 */
PointerDetectorAdvFilter.prototype.clearWindows = function(callback){
  return this.invoke('clearWindows', callback);
};
/**
 * @callback PointerDetectorAdvFilter~clearWindowsCallback
 * @param {Error} error
 */

/**
 * Removes a window from the list to be monitored
 *
 * @param {String} windowId
 *  the id of the window to be removed
 *
 * @param {module:kwsMediaApi/filters~PointerDetectorAdvFilter.removeWindowCallback} [callback]
 *
 * @return {module:kwsMediaApi/filters~PointerDetectorAdvFilter}
 *  The own media object
 */
PointerDetectorAdvFilter.prototype.removeWindow = function(windowId, callback){
  checkType('String', 'windowId', windowId, {required: true});

  var params = {
    windowId: windowId,
  };

  return this.invoke('removeWindow', params, callback);
};
/**
 * @callback PointerDetectorAdvFilter~removeWindowCallback
 * @param {Error} error
 */

/**
 * This method allows to calibrate the tracking color.

The new tracking color will be the color of the object in the colorCalibrationRegion.
 *
 * @param {module:kwsMediaApi/filters~PointerDetectorAdvFilter.trackColorFromCalibrationRegionCallback} [callback]
 *
 * @return {module:kwsMediaApi/filters~PointerDetectorAdvFilter}
 *  The own media object
 */
PointerDetectorAdvFilter.prototype.trackColorFromCalibrationRegion = function(callback){
  return this.invoke('trackColorFromCalibrationRegion', callback);
};
/**
 * @callback PointerDetectorAdvFilter~trackColorFromCalibrationRegionCallback
 * @param {Error} error
 */


/**
 * @type module:kwsMediaApi/filters~PointerDetectorAdvFilter.constructorParams
 *
 * @property {WindowParam} calibrationRegion
 *  region to calibrate the filter
 *
 * @property {MediaPipeline} mediaPipeline
 *  the :rom:cls:`MediaPipeline` to which the filter belongs
 *
 * @property {PointerDetectorWindowMediaParam} [windows]
 *  list of detection windows for the filter.
 */
PointerDetectorAdvFilter.constructorParams = {
  calibrationRegion: {
    type: 'WindowParam',
    required: true
  },

  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },

  windows: {
    type: 'PointerDetectorWindowMediaParam',
    isList: true,
  },
};

/**
 * @type   module:kwsMediaApi/filters~PointerDetectorAdvFilter.events
 * @extend module:kwsMediaApi~Filter.events
 */
PointerDetectorAdvFilter.events = ['WindowIn', 'WindowOut'];
PointerDetectorAdvFilter.events.concat(Filter.events);


module.exports = PointerDetectorAdvFilter;


PointerDetectorAdvFilter.check = function(key, value)
{
  if(!(value instanceof PointerDetectorAdvFilter))
    throw SyntaxError(key+' param should be a PointerDetectorAdvFilter, not '+typeof value);
};

},{"../checkType":3,"../core/Filter":13,"inherits":68}],41:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var checkType = require('../checkType');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/filters
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var Filter = require('../core/Filter');


/**
 * This type of :rom:cls:`Filter` detects pointers in a video feed.
 *
 * @class   module:kwsMediaApi/filters~PointerDetectorFilter
 * @extends module:kwsMediaApi~Filter
 */

/**
 * Builder for the :rom:cls:`PointerDetectorFilter`.
 *
 * @constructor
 *
 * @param {string} id
 */
function PointerDetectorFilter(id)
{
  Filter.call(this, id);
};
inherits(PointerDetectorFilter, Filter);


/**
 * Adds a pointer detector window. When a pointer enters or exits this window, the filter will raise an event indicating so.
 *
 * @param {PointerDetectorWindowMediaParam} window
 *  the detection window
 *
 * @param {module:kwsMediaApi/filters~PointerDetectorFilter.addWindowCallback} [callback]
 *
 * @return {module:kwsMediaApi/filters~PointerDetectorFilter}
 *  The own media object
 */
PointerDetectorFilter.prototype.addWindow = function(window, callback){
  checkType('PointerDetectorWindowMediaParam', 'window', window, {required: true});

  var params = {
    window: window,
  };

  return this.invoke('addWindow', params, callback);
};
/**
 * @callback PointerDetectorFilter~addWindowCallback
 * @param {Error} error
 */

/**
 * Removes all pointer detector windows
 *
 * @param {module:kwsMediaApi/filters~PointerDetectorFilter.clearWindowsCallback} [callback]
 *
 * @return {module:kwsMediaApi/filters~PointerDetectorFilter}
 *  The own media object
 */
PointerDetectorFilter.prototype.clearWindows = function(callback){
  return this.invoke('clearWindows', callback);
};
/**
 * @callback PointerDetectorFilter~clearWindowsCallback
 * @param {Error} error
 */

/**
 * Removes a pointer detector window
 *
 * @param {String} windowId
 *  id of the window to be removed
 *
 * @param {module:kwsMediaApi/filters~PointerDetectorFilter.removeWindowCallback} [callback]
 *
 * @return {module:kwsMediaApi/filters~PointerDetectorFilter}
 *  The own media object
 */
PointerDetectorFilter.prototype.removeWindow = function(windowId, callback){
  checkType('String', 'windowId', windowId, {required: true});

  var params = {
    windowId: windowId,
  };

  return this.invoke('removeWindow', params, callback);
};
/**
 * @callback PointerDetectorFilter~removeWindowCallback
 * @param {Error} error
 */


/**
 * @type module:kwsMediaApi/filters~PointerDetectorFilter.constructorParams
 *
 * @property {MediaPipeline} mediaPipeline
 *  the :rom:cls:`MediaPipeline` to which the filter belongs
 *
 * @property {PointerDetectorWindowMediaParam} [windows]
 *  list of detection windows for the filter to detect pointers entering or exiting the window
 */
PointerDetectorFilter.constructorParams = {
  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },

  windows: {
    type: 'PointerDetectorWindowMediaParam',
    isList: true,
  },
};

/**
 * @type   module:kwsMediaApi/filters~PointerDetectorFilter.events
 * @extend module:kwsMediaApi~Filter.events
 */
PointerDetectorFilter.events = ['WindowIn', 'WindowOut'];
PointerDetectorFilter.events.concat(Filter.events);


module.exports = PointerDetectorFilter;


PointerDetectorFilter.check = function(key, value)
{
  if(!(value instanceof PointerDetectorFilter))
    throw SyntaxError(key+' param should be a PointerDetectorFilter, not '+typeof value);
};

},{"../checkType":3,"../core/Filter":13,"inherits":68}],42:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/filters
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var Filter = require('../core/Filter');


/**
 * This filter detects :term:`QR` codes in a video feed. When a code is found, the filter raises a :rom:evnt:`CodeFound` event.
 *
 * @class   module:kwsMediaApi/filters~ZBarFilter
 * @extends module:kwsMediaApi~Filter
 */

/**
 * Builder for the :rom:cls:`ZBarFilter`.
 *
 * @constructor
 *
 * @param {string} id
 */
function ZBarFilter(id)
{
  Filter.call(this, id);
};
inherits(ZBarFilter, Filter);


/**
 * @type module:kwsMediaApi/filters~ZBarFilter.constructorParams
 *
 * @property {MediaPipeline} mediaPipeline
 *  the :rom:cls:`MediaPipeline` to which the filter belongs
 */
ZBarFilter.constructorParams = {
  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },
};

/**
 * @type   module:kwsMediaApi/filters~ZBarFilter.events
 * @extend module:kwsMediaApi~Filter.events
 */
ZBarFilter.events = ['CodeFound'];
ZBarFilter.events.concat(Filter.events);


module.exports = ZBarFilter;


ZBarFilter.check = function(key, value)
{
  if(!(value instanceof ZBarFilter))
    throw SyntaxError(key+' param should be a ZBarFilter, not '+typeof value);
};

},{"../core/Filter":13,"inherits":68}],43:[function(require,module,exports){
/*
 * (C) Copyright 2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/filters
 *
 * @copyright 2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var ChromaFilter = require('./ChromaFilter');
var CrowdDetectorFilter = require('./CrowdDetectorFilter');
var FaceOverlayFilter = require('./FaceOverlayFilter');
var GStreamerFilter = require('./GStreamerFilter');
var JackVaderFilter = require('./JackVaderFilter');
var PlateDetectorFilter = require('./PlateDetectorFilter');
var PointerDetectorAdvFilter = require('./PointerDetectorAdvFilter');
var PointerDetectorFilter = require('./PointerDetectorFilter');
var ZBarFilter = require('./ZBarFilter');


exports.ChromaFilter = ChromaFilter;
exports.CrowdDetectorFilter = CrowdDetectorFilter;
exports.FaceOverlayFilter = FaceOverlayFilter;
exports.GStreamerFilter = GStreamerFilter;
exports.JackVaderFilter = JackVaderFilter;
exports.PlateDetectorFilter = PlateDetectorFilter;
exports.PointerDetectorAdvFilter = PointerDetectorAdvFilter;
exports.PointerDetectorFilter = PointerDetectorFilter;
exports.ZBarFilter = ZBarFilter;

},{"./ChromaFilter":34,"./CrowdDetectorFilter":35,"./FaceOverlayFilter":36,"./GStreamerFilter":37,"./JackVaderFilter":38,"./PlateDetectorFilter":39,"./PointerDetectorAdvFilter":40,"./PointerDetectorFilter":41,"./ZBarFilter":42}],44:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/hubs
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var Hub = require('../core/Hub');


/**
 * A :rom:cls:`Hub` that mixes the :rom:attr:`MediaType.AUDIO` stream of its connected sources and constructs a grid with the :rom:attr:`MediaType.VIDEO` streams of its connected sources into its sink
 *
 * @class   module:kwsMediaApi/hubs~Composite
 * @extends module:kwsMediaApi~Hub
 */

/**
 * Create for the given pipeline
 *
 * @constructor
 *
 * @param {string} id
 */
function Composite(id)
{
  Hub.call(this, id);
};
inherits(Composite, Hub);


/**
 * @type module:kwsMediaApi/hubs~Composite.constructorParams
 *
 * @property {MediaPipeline} mediaPipeline
 *  the :rom:cls:`MediaPipeline` to which the dispatcher belongs
 */
Composite.constructorParams = {
  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },
};

/**
 * @type   module:kwsMediaApi/hubs~Composite.events
 * @extend module:kwsMediaApi~Hub.events
 */
Composite.events = [];
Composite.events.concat(Hub.events);


module.exports = Composite;


Composite.check = function(key, value)
{
  if(!(value instanceof Composite))
    throw SyntaxError(key+' param should be a Composite, not '+typeof value);
};

},{"../core/Hub":14,"inherits":68}],45:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var checkType = require('../checkType');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/hubs
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var Hub = require('../core/Hub');


/**
 * A :rom:cls:`Hub` that allows routing between arbitrary port pairs
 *
 * @class   module:kwsMediaApi/hubs~Dispatcher
 * @extends module:kwsMediaApi~Hub
 */

/**
 * Create a :rom:cls:`Dispatcher` belonging to the given pipeline.
 *
 * @constructor
 *
 * @param {string} id
 */
function Dispatcher(id)
{
  Hub.call(this, id);
};
inherits(Dispatcher, Hub);


/**
 * Connects each corresponding :rom:enum:`MediaType` of the given source port with the sink port.
 *
 * @param {HubPort} source
 *  Source port to be connected
 *
 * @param {HubPort} sink
 *  Sink port to be connected
 *
 * @param {module:kwsMediaApi/hubs~Dispatcher.connectCallback} [callback]
 *
 * @return {module:kwsMediaApi/hubs~Dispatcher}
 *  The own media object
 */
Dispatcher.prototype.connect = function(source, sink, callback){
  checkType('HubPort', 'source', source, {required: true});
  checkType('HubPort', 'sink', sink, {required: true});

  var params = {
    source: source,
    sink: sink,
  };

  return this.invoke('connect', params, callback);
};
/**
 * @callback Dispatcher~connectCallback
 * @param {Error} error
 */


/**
 * @type module:kwsMediaApi/hubs~Dispatcher.constructorParams
 *
 * @property {MediaPipeline} mediaPipeline
 *  the :rom:cls:`MediaPipeline` to which the dispatcher belongs
 */
Dispatcher.constructorParams = {
  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },
};

/**
 * @type   module:kwsMediaApi/hubs~Dispatcher.events
 * @extend module:kwsMediaApi~Hub.events
 */
Dispatcher.events = [];
Dispatcher.events.concat(Hub.events);


module.exports = Dispatcher;


Dispatcher.check = function(key, value)
{
  if(!(value instanceof Dispatcher))
    throw SyntaxError(key+' param should be a Dispatcher, not '+typeof value);
};

},{"../checkType":3,"../core/Hub":14,"inherits":68}],46:[function(require,module,exports){
/*
 * (C) Copyright 2013-2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var inherits = require('inherits');

var checkType = require('../checkType');


/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/hubs
 *
 * @copyright 2013-2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var Hub = require('../core/Hub');


/**
 * A :rom:cls:`Hub` that sends a given source to all the connected sinks
 *
 * @class   module:kwsMediaApi/hubs~DispatcherOneToMany
 * @extends module:kwsMediaApi~Hub
 */

/**
 * Create a :rom:cls:`DispatcherOneToMany` belonging to the given pipeline.
 *
 * @constructor
 *
 * @param {string} id
 */
function DispatcherOneToMany(id)
{
  Hub.call(this, id);
};
inherits(DispatcherOneToMany, Hub);


/**
 * Remove the source port and stop the media pipeline.
 *
 * @param {module:kwsMediaApi/hubs~DispatcherOneToMany.removeSourceCallback} [callback]
 *
 * @return {module:kwsMediaApi/hubs~DispatcherOneToMany}
 *  The own media object
 */
DispatcherOneToMany.prototype.removeSource = function(callback){
  return this.invoke('removeSource', callback);
};
/**
 * @callback DispatcherOneToMany~removeSourceCallback
 * @param {Error} error
 */

/**
 * Sets the source port that will be connected to the sinks of every :rom:cls:`HubPort` of the dispatcher
 *
 * @param {HubPort} source
 *  source to be broadcasted
 *
 * @param {module:kwsMediaApi/hubs~DispatcherOneToMany.setSourceCallback} [callback]
 *
 * @return {module:kwsMediaApi/hubs~DispatcherOneToMany}
 *  The own media object
 */
DispatcherOneToMany.prototype.setSource = function(source, callback){
  checkType('HubPort', 'source', source, {required: true});

  var params = {
    source: source,
  };

  return this.invoke('setSource', params, callback);
};
/**
 * @callback DispatcherOneToMany~setSourceCallback
 * @param {Error} error
 */


/**
 * @type module:kwsMediaApi/hubs~DispatcherOneToMany.constructorParams
 *
 * @property {MediaPipeline} mediaPipeline
 *  the :rom:cls:`MediaPipeline` to which the dispatcher belongs
 */
DispatcherOneToMany.constructorParams = {
  mediaPipeline: {
    type: 'MediaPipeline',
    required: true
  },
};

/**
 * @type   module:kwsMediaApi/hubs~DispatcherOneToMany.events
 * @extend module:kwsMediaApi~Hub.events
 */
DispatcherOneToMany.events = [];
DispatcherOneToMany.events.concat(Hub.events);


module.exports = DispatcherOneToMany;


DispatcherOneToMany.check = function(key, value)
{
  if(!(value instanceof DispatcherOneToMany))
    throw SyntaxError(key+' param should be a DispatcherOneToMany, not '+typeof value);
};

},{"../checkType":3,"../core/Hub":14,"inherits":68}],47:[function(require,module,exports){
/*
 * (C) Copyright 2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

/**
 * Media API for the Kurento Web SDK
 *
 * @module kwsMediaApi/hubs
 *
 * @copyright 2014 Kurento (http://kurento.org/)
 * @license LGPL
 */

var Composite = require('./Composite');
var Dispatcher = require('./Dispatcher');
var DispatcherOneToMany = require('./DispatcherOneToMany');


exports.Composite = Composite;
exports.Dispatcher = Dispatcher;
exports.DispatcherOneToMany = DispatcherOneToMany;

},{"./Composite":44,"./Dispatcher":45,"./DispatcherOneToMany":46}],48:[function(require,module,exports){
/*
 * (C) Copyright 2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */


function noop(error)
{
  if(error) console.error(error);
};


/**
 * Define a callback as the continuation of a promise
 */
function promiseCallback(promise, callback)
{
  if(callback)
  {
    function callback2(error, result)
    {
      try
      {
        callback(error, result);
      }
      catch(exception)
      {
        // Show the exception in the console with its full stack trace
        console.error(exception);
        throw exception;
      }
    };
  
    promise.then(function(result)
    {
      callback2(null, result);
    },
    callback2);
  };
};


exports.noop            = noop;
exports.promiseCallback = promiseCallback;

},{}],49:[function(require,module,exports){
(function (process){
/*!
 * async
 * https://github.com/caolan/async
 *
 * Copyright 2010-2014 Caolan McMahon
 * Released under the MIT license
 */
/*jshint onevar: false, indent:4 */
/*global setImmediate: false, setTimeout: false, console: false */
(function () {

    var async = {};

    // global on the server, window in the browser
    var root, previous_async;

    root = this;
    if (root != null) {
      previous_async = root.async;
    }

    async.noConflict = function () {
        root.async = previous_async;
        return async;
    };

    function only_once(fn) {
        var called = false;
        return function() {
            if (called) throw new Error("Callback was already called.");
            called = true;
            fn.apply(root, arguments);
        }
    }

    //// cross-browser compatiblity functions ////

    var _toString = Object.prototype.toString;

    var _isArray = Array.isArray || function (obj) {
        return _toString.call(obj) === '[object Array]';
    };

    var _each = function (arr, iterator) {
        if (arr.forEach) {
            return arr.forEach(iterator);
        }
        for (var i = 0; i < arr.length; i += 1) {
            iterator(arr[i], i, arr);
        }
    };

    var _map = function (arr, iterator) {
        if (arr.map) {
            return arr.map(iterator);
        }
        var results = [];
        _each(arr, function (x, i, a) {
            results.push(iterator(x, i, a));
        });
        return results;
    };

    var _reduce = function (arr, iterator, memo) {
        if (arr.reduce) {
            return arr.reduce(iterator, memo);
        }
        _each(arr, function (x, i, a) {
            memo = iterator(memo, x, i, a);
        });
        return memo;
    };

    var _keys = function (obj) {
        if (Object.keys) {
            return Object.keys(obj);
        }
        var keys = [];
        for (var k in obj) {
            if (obj.hasOwnProperty(k)) {
                keys.push(k);
            }
        }
        return keys;
    };

    //// exported async module functions ////

    //// nextTick implementation with browser-compatible fallback ////
    if (typeof process === 'undefined' || !(process.nextTick)) {
        if (typeof setImmediate === 'function') {
            async.nextTick = function (fn) {
                // not a direct alias for IE10 compatibility
                setImmediate(fn);
            };
            async.setImmediate = async.nextTick;
        }
        else {
            async.nextTick = function (fn) {
                setTimeout(fn, 0);
            };
            async.setImmediate = async.nextTick;
        }
    }
    else {
        async.nextTick = process.nextTick;
        if (typeof setImmediate !== 'undefined') {
            async.setImmediate = function (fn) {
              // not a direct alias for IE10 compatibility
              setImmediate(fn);
            };
        }
        else {
            async.setImmediate = async.nextTick;
        }
    }

    async.each = function (arr, iterator, callback) {
        callback = callback || function () {};
        if (!arr.length) {
            return callback();
        }
        var completed = 0;
        _each(arr, function (x) {
            iterator(x, only_once(done) );
        });
        function done(err) {
          if (err) {
              callback(err);
              callback = function () {};
          }
          else {
              completed += 1;
              if (completed >= arr.length) {
                  callback();
              }
          }
        }
    };
    async.forEach = async.each;

    async.eachSeries = function (arr, iterator, callback) {
        callback = callback || function () {};
        if (!arr.length) {
            return callback();
        }
        var completed = 0;
        var iterate = function () {
            iterator(arr[completed], function (err) {
                if (err) {
                    callback(err);
                    callback = function () {};
                }
                else {
                    completed += 1;
                    if (completed >= arr.length) {
                        callback();
                    }
                    else {
                        iterate();
                    }
                }
            });
        };
        iterate();
    };
    async.forEachSeries = async.eachSeries;

    async.eachLimit = function (arr, limit, iterator, callback) {
        var fn = _eachLimit(limit);
        fn.apply(null, [arr, iterator, callback]);
    };
    async.forEachLimit = async.eachLimit;

    var _eachLimit = function (limit) {

        return function (arr, iterator, callback) {
            callback = callback || function () {};
            if (!arr.length || limit <= 0) {
                return callback();
            }
            var completed = 0;
            var started = 0;
            var running = 0;

            (function replenish () {
                if (completed >= arr.length) {
                    return callback();
                }

                while (running < limit && started < arr.length) {
                    started += 1;
                    running += 1;
                    iterator(arr[started - 1], function (err) {
                        if (err) {
                            callback(err);
                            callback = function () {};
                        }
                        else {
                            completed += 1;
                            running -= 1;
                            if (completed >= arr.length) {
                                callback();
                            }
                            else {
                                replenish();
                            }
                        }
                    });
                }
            })();
        };
    };


    var doParallel = function (fn) {
        return function () {
            var args = Array.prototype.slice.call(arguments);
            return fn.apply(null, [async.each].concat(args));
        };
    };
    var doParallelLimit = function(limit, fn) {
        return function () {
            var args = Array.prototype.slice.call(arguments);
            return fn.apply(null, [_eachLimit(limit)].concat(args));
        };
    };
    var doSeries = function (fn) {
        return function () {
            var args = Array.prototype.slice.call(arguments);
            return fn.apply(null, [async.eachSeries].concat(args));
        };
    };


    var _asyncMap = function (eachfn, arr, iterator, callback) {
        arr = _map(arr, function (x, i) {
            return {index: i, value: x};
        });
        if (!callback) {
            eachfn(arr, function (x, callback) {
                iterator(x.value, function (err) {
                    callback(err);
                });
            });
        } else {
            var results = [];
            eachfn(arr, function (x, callback) {
                iterator(x.value, function (err, v) {
                    results[x.index] = v;
                    callback(err);
                });
            }, function (err) {
                callback(err, results);
            });
        }
    };
    async.map = doParallel(_asyncMap);
    async.mapSeries = doSeries(_asyncMap);
    async.mapLimit = function (arr, limit, iterator, callback) {
        return _mapLimit(limit)(arr, iterator, callback);
    };

    var _mapLimit = function(limit) {
        return doParallelLimit(limit, _asyncMap);
    };

    // reduce only has a series version, as doing reduce in parallel won't
    // work in many situations.
    async.reduce = function (arr, memo, iterator, callback) {
        async.eachSeries(arr, function (x, callback) {
            iterator(memo, x, function (err, v) {
                memo = v;
                callback(err);
            });
        }, function (err) {
            callback(err, memo);
        });
    };
    // inject alias
    async.inject = async.reduce;
    // foldl alias
    async.foldl = async.reduce;

    async.reduceRight = function (arr, memo, iterator, callback) {
        var reversed = _map(arr, function (x) {
            return x;
        }).reverse();
        async.reduce(reversed, memo, iterator, callback);
    };
    // foldr alias
    async.foldr = async.reduceRight;

    var _filter = function (eachfn, arr, iterator, callback) {
        var results = [];
        arr = _map(arr, function (x, i) {
            return {index: i, value: x};
        });
        eachfn(arr, function (x, callback) {
            iterator(x.value, function (v) {
                if (v) {
                    results.push(x);
                }
                callback();
            });
        }, function (err) {
            callback(_map(results.sort(function (a, b) {
                return a.index - b.index;
            }), function (x) {
                return x.value;
            }));
        });
    };
    async.filter = doParallel(_filter);
    async.filterSeries = doSeries(_filter);
    // select alias
    async.select = async.filter;
    async.selectSeries = async.filterSeries;

    var _reject = function (eachfn, arr, iterator, callback) {
        var results = [];
        arr = _map(arr, function (x, i) {
            return {index: i, value: x};
        });
        eachfn(arr, function (x, callback) {
            iterator(x.value, function (v) {
                if (!v) {
                    results.push(x);
                }
                callback();
            });
        }, function (err) {
            callback(_map(results.sort(function (a, b) {
                return a.index - b.index;
            }), function (x) {
                return x.value;
            }));
        });
    };
    async.reject = doParallel(_reject);
    async.rejectSeries = doSeries(_reject);

    var _detect = function (eachfn, arr, iterator, main_callback) {
        eachfn(arr, function (x, callback) {
            iterator(x, function (result) {
                if (result) {
                    main_callback(x);
                    main_callback = function () {};
                }
                else {
                    callback();
                }
            });
        }, function (err) {
            main_callback();
        });
    };
    async.detect = doParallel(_detect);
    async.detectSeries = doSeries(_detect);

    async.some = function (arr, iterator, main_callback) {
        async.each(arr, function (x, callback) {
            iterator(x, function (v) {
                if (v) {
                    main_callback(true);
                    main_callback = function () {};
                }
                callback();
            });
        }, function (err) {
            main_callback(false);
        });
    };
    // any alias
    async.any = async.some;

    async.every = function (arr, iterator, main_callback) {
        async.each(arr, function (x, callback) {
            iterator(x, function (v) {
                if (!v) {
                    main_callback(false);
                    main_callback = function () {};
                }
                callback();
            });
        }, function (err) {
            main_callback(true);
        });
    };
    // all alias
    async.all = async.every;

    async.sortBy = function (arr, iterator, callback) {
        async.map(arr, function (x, callback) {
            iterator(x, function (err, criteria) {
                if (err) {
                    callback(err);
                }
                else {
                    callback(null, {value: x, criteria: criteria});
                }
            });
        }, function (err, results) {
            if (err) {
                return callback(err);
            }
            else {
                var fn = function (left, right) {
                    var a = left.criteria, b = right.criteria;
                    return a < b ? -1 : a > b ? 1 : 0;
                };
                callback(null, _map(results.sort(fn), function (x) {
                    return x.value;
                }));
            }
        });
    };

    async.auto = function (tasks, callback) {
        callback = callback || function () {};
        var keys = _keys(tasks);
        var remainingTasks = keys.length
        if (!remainingTasks) {
            return callback();
        }

        var results = {};

        var listeners = [];
        var addListener = function (fn) {
            listeners.unshift(fn);
        };
        var removeListener = function (fn) {
            for (var i = 0; i < listeners.length; i += 1) {
                if (listeners[i] === fn) {
                    listeners.splice(i, 1);
                    return;
                }
            }
        };
        var taskComplete = function () {
            remainingTasks--
            _each(listeners.slice(0), function (fn) {
                fn();
            });
        };

        addListener(function () {
            if (!remainingTasks) {
                var theCallback = callback;
                // prevent final callback from calling itself if it errors
                callback = function () {};

                theCallback(null, results);
            }
        });

        _each(keys, function (k) {
            var task = _isArray(tasks[k]) ? tasks[k]: [tasks[k]];
            var taskCallback = function (err) {
                var args = Array.prototype.slice.call(arguments, 1);
                if (args.length <= 1) {
                    args = args[0];
                }
                if (err) {
                    var safeResults = {};
                    _each(_keys(results), function(rkey) {
                        safeResults[rkey] = results[rkey];
                    });
                    safeResults[k] = args;
                    callback(err, safeResults);
                    // stop subsequent errors hitting callback multiple times
                    callback = function () {};
                }
                else {
                    results[k] = args;
                    async.setImmediate(taskComplete);
                }
            };
            var requires = task.slice(0, Math.abs(task.length - 1)) || [];
            var ready = function () {
                return _reduce(requires, function (a, x) {
                    return (a && results.hasOwnProperty(x));
                }, true) && !results.hasOwnProperty(k);
            };
            if (ready()) {
                task[task.length - 1](taskCallback, results);
            }
            else {
                var listener = function () {
                    if (ready()) {
                        removeListener(listener);
                        task[task.length - 1](taskCallback, results);
                    }
                };
                addListener(listener);
            }
        });
    };

    async.retry = function(times, task, callback) {
        var DEFAULT_TIMES = 5;
        var attempts = [];
        // Use defaults if times not passed
        if (typeof times === 'function') {
            callback = task;
            task = times;
            times = DEFAULT_TIMES;
        }
        // Make sure times is a number
        times = parseInt(times, 10) || DEFAULT_TIMES;
        var wrappedTask = function(wrappedCallback, wrappedResults) {
            var retryAttempt = function(task, finalAttempt) {
                return function(seriesCallback) {
                    task(function(err, result){
                        seriesCallback(!err || finalAttempt, {err: err, result: result});
                    }, wrappedResults);
                };
            };
            while (times) {
                attempts.push(retryAttempt(task, !(times-=1)));
            }
            async.series(attempts, function(done, data){
                data = data[data.length - 1];
                (wrappedCallback || callback)(data.err, data.result);
            });
        }
        // If a callback is passed, run this as a controll flow
        return callback ? wrappedTask() : wrappedTask
    };

    async.waterfall = function (tasks, callback) {
        callback = callback || function () {};
        if (!_isArray(tasks)) {
          var err = new Error('First argument to waterfall must be an array of functions');
          return callback(err);
        }
        if (!tasks.length) {
            return callback();
        }
        var wrapIterator = function (iterator) {
            return function (err) {
                if (err) {
                    callback.apply(null, arguments);
                    callback = function () {};
                }
                else {
                    var args = Array.prototype.slice.call(arguments, 1);
                    var next = iterator.next();
                    if (next) {
                        args.push(wrapIterator(next));
                    }
                    else {
                        args.push(callback);
                    }
                    async.setImmediate(function () {
                        iterator.apply(null, args);
                    });
                }
            };
        };
        wrapIterator(async.iterator(tasks))();
    };

    var _parallel = function(eachfn, tasks, callback) {
        callback = callback || function () {};
        if (_isArray(tasks)) {
            eachfn.map(tasks, function (fn, callback) {
                if (fn) {
                    fn(function (err) {
                        var args = Array.prototype.slice.call(arguments, 1);
                        if (args.length <= 1) {
                            args = args[0];
                        }
                        callback.call(null, err, args);
                    });
                }
            }, callback);
        }
        else {
            var results = {};
            eachfn.each(_keys(tasks), function (k, callback) {
                tasks[k](function (err) {
                    var args = Array.prototype.slice.call(arguments, 1);
                    if (args.length <= 1) {
                        args = args[0];
                    }
                    results[k] = args;
                    callback(err);
                });
            }, function (err) {
                callback(err, results);
            });
        }
    };

    async.parallel = function (tasks, callback) {
        _parallel({ map: async.map, each: async.each }, tasks, callback);
    };

    async.parallelLimit = function(tasks, limit, callback) {
        _parallel({ map: _mapLimit(limit), each: _eachLimit(limit) }, tasks, callback);
    };

    async.series = function (tasks, callback) {
        callback = callback || function () {};
        if (_isArray(tasks)) {
            async.mapSeries(tasks, function (fn, callback) {
                if (fn) {
                    fn(function (err) {
                        var args = Array.prototype.slice.call(arguments, 1);
                        if (args.length <= 1) {
                            args = args[0];
                        }
                        callback.call(null, err, args);
                    });
                }
            }, callback);
        }
        else {
            var results = {};
            async.eachSeries(_keys(tasks), function (k, callback) {
                tasks[k](function (err) {
                    var args = Array.prototype.slice.call(arguments, 1);
                    if (args.length <= 1) {
                        args = args[0];
                    }
                    results[k] = args;
                    callback(err);
                });
            }, function (err) {
                callback(err, results);
            });
        }
    };

    async.iterator = function (tasks) {
        var makeCallback = function (index) {
            var fn = function () {
                if (tasks.length) {
                    tasks[index].apply(null, arguments);
                }
                return fn.next();
            };
            fn.next = function () {
                return (index < tasks.length - 1) ? makeCallback(index + 1): null;
            };
            return fn;
        };
        return makeCallback(0);
    };

    async.apply = function (fn) {
        var args = Array.prototype.slice.call(arguments, 1);
        return function () {
            return fn.apply(
                null, args.concat(Array.prototype.slice.call(arguments))
            );
        };
    };

    var _concat = function (eachfn, arr, fn, callback) {
        var r = [];
        eachfn(arr, function (x, cb) {
            fn(x, function (err, y) {
                r = r.concat(y || []);
                cb(err);
            });
        }, function (err) {
            callback(err, r);
        });
    };
    async.concat = doParallel(_concat);
    async.concatSeries = doSeries(_concat);

    async.whilst = function (test, iterator, callback) {
        if (test()) {
            iterator(function (err) {
                if (err) {
                    return callback(err);
                }
                async.whilst(test, iterator, callback);
            });
        }
        else {
            callback();
        }
    };

    async.doWhilst = function (iterator, test, callback) {
        iterator(function (err) {
            if (err) {
                return callback(err);
            }
            var args = Array.prototype.slice.call(arguments, 1);
            if (test.apply(null, args)) {
                async.doWhilst(iterator, test, callback);
            }
            else {
                callback();
            }
        });
    };

    async.until = function (test, iterator, callback) {
        if (!test()) {
            iterator(function (err) {
                if (err) {
                    return callback(err);
                }
                async.until(test, iterator, callback);
            });
        }
        else {
            callback();
        }
    };

    async.doUntil = function (iterator, test, callback) {
        iterator(function (err) {
            if (err) {
                return callback(err);
            }
            var args = Array.prototype.slice.call(arguments, 1);
            if (!test.apply(null, args)) {
                async.doUntil(iterator, test, callback);
            }
            else {
                callback();
            }
        });
    };

    async.queue = function (worker, concurrency) {
        if (concurrency === undefined) {
            concurrency = 1;
        }
        function _insert(q, data, pos, callback) {
          if (!q.started){
            q.started = true;
          }
          if (!_isArray(data)) {
              data = [data];
          }
          if(data.length == 0) {
             // call drain immediately if there are no tasks
             return async.setImmediate(function() {
                 if (q.drain) {
                     q.drain();
                 }
             });
          }
          _each(data, function(task) {
              var item = {
                  data: task,
                  callback: typeof callback === 'function' ? callback : null
              };

              if (pos) {
                q.tasks.unshift(item);
              } else {
                q.tasks.push(item);
              }

              if (q.saturated && q.tasks.length === q.concurrency) {
                  q.saturated();
              }
              async.setImmediate(q.process);
          });
        }

        var workers = 0;
        var q = {
            tasks: [],
            concurrency: concurrency,
            saturated: null,
            empty: null,
            drain: null,
            started: false,
            paused: false,
            push: function (data, callback) {
              _insert(q, data, false, callback);
            },
            kill: function () {
              q.drain = null;
              q.tasks = [];
            },
            unshift: function (data, callback) {
              _insert(q, data, true, callback);
            },
            process: function () {
                if (!q.paused && workers < q.concurrency && q.tasks.length) {
                    var task = q.tasks.shift();
                    if (q.empty && q.tasks.length === 0) {
                        q.empty();
                    }
                    workers += 1;
                    var next = function () {
                        workers -= 1;
                        if (task.callback) {
                            task.callback.apply(task, arguments);
                        }
                        if (q.drain && q.tasks.length + workers === 0) {
                            q.drain();
                        }
                        q.process();
                    };
                    var cb = only_once(next);
                    worker(task.data, cb);
                }
            },
            length: function () {
                return q.tasks.length;
            },
            running: function () {
                return workers;
            },
            idle: function() {
                return q.tasks.length + workers === 0;
            },
            pause: function () {
                if (q.paused === true) { return; }
                q.paused = true;
                q.process();
            },
            resume: function () {
                if (q.paused === false) { return; }
                q.paused = false;
                q.process();
            }
        };
        return q;
    };
    
    async.priorityQueue = function (worker, concurrency) {
        
        function _compareTasks(a, b){
          return a.priority - b.priority;
        };
        
        function _binarySearch(sequence, item, compare) {
          var beg = -1,
              end = sequence.length - 1;
          while (beg < end) {
            var mid = beg + ((end - beg + 1) >>> 1);
            if (compare(item, sequence[mid]) >= 0) {
              beg = mid;
            } else {
              end = mid - 1;
            }
          }
          return beg;
        }
        
        function _insert(q, data, priority, callback) {
          if (!q.started){
            q.started = true;
          }
          if (!_isArray(data)) {
              data = [data];
          }
          if(data.length == 0) {
             // call drain immediately if there are no tasks
             return async.setImmediate(function() {
                 if (q.drain) {
                     q.drain();
                 }
             });
          }
          _each(data, function(task) {
              var item = {
                  data: task,
                  priority: priority,
                  callback: typeof callback === 'function' ? callback : null
              };
              
              q.tasks.splice(_binarySearch(q.tasks, item, _compareTasks) + 1, 0, item);

              if (q.saturated && q.tasks.length === q.concurrency) {
                  q.saturated();
              }
              async.setImmediate(q.process);
          });
        }
        
        // Start with a normal queue
        var q = async.queue(worker, concurrency);
        
        // Override push to accept second parameter representing priority
        q.push = function (data, priority, callback) {
          _insert(q, data, priority, callback);
        };
        
        // Remove unshift function
        delete q.unshift;

        return q;
    };

    async.cargo = function (worker, payload) {
        var working     = false,
            tasks       = [];

        var cargo = {
            tasks: tasks,
            payload: payload,
            saturated: null,
            empty: null,
            drain: null,
            drained: true,
            push: function (data, callback) {
                if (!_isArray(data)) {
                    data = [data];
                }
                _each(data, function(task) {
                    tasks.push({
                        data: task,
                        callback: typeof callback === 'function' ? callback : null
                    });
                    cargo.drained = false;
                    if (cargo.saturated && tasks.length === payload) {
                        cargo.saturated();
                    }
                });
                async.setImmediate(cargo.process);
            },
            process: function process() {
                if (working) return;
                if (tasks.length === 0) {
                    if(cargo.drain && !cargo.drained) cargo.drain();
                    cargo.drained = true;
                    return;
                }

                var ts = typeof payload === 'number'
                            ? tasks.splice(0, payload)
                            : tasks.splice(0, tasks.length);

                var ds = _map(ts, function (task) {
                    return task.data;
                });

                if(cargo.empty) cargo.empty();
                working = true;
                worker(ds, function () {
                    working = false;

                    var args = arguments;
                    _each(ts, function (data) {
                        if (data.callback) {
                            data.callback.apply(null, args);
                        }
                    });

                    process();
                });
            },
            length: function () {
                return tasks.length;
            },
            running: function () {
                return working;
            }
        };
        return cargo;
    };

    var _console_fn = function (name) {
        return function (fn) {
            var args = Array.prototype.slice.call(arguments, 1);
            fn.apply(null, args.concat([function (err) {
                var args = Array.prototype.slice.call(arguments, 1);
                if (typeof console !== 'undefined') {
                    if (err) {
                        if (console.error) {
                            console.error(err);
                        }
                    }
                    else if (console[name]) {
                        _each(args, function (x) {
                            console[name](x);
                        });
                    }
                }
            }]));
        };
    };
    async.log = _console_fn('log');
    async.dir = _console_fn('dir');
    /*async.info = _console_fn('info');
    async.warn = _console_fn('warn');
    async.error = _console_fn('error');*/

    async.memoize = function (fn, hasher) {
        var memo = {};
        var queues = {};
        hasher = hasher || function (x) {
            return x;
        };
        var memoized = function () {
            var args = Array.prototype.slice.call(arguments);
            var callback = args.pop();
            var key = hasher.apply(null, args);
            if (key in memo) {
                async.nextTick(function () {
                    callback.apply(null, memo[key]);
                });
            }
            else if (key in queues) {
                queues[key].push(callback);
            }
            else {
                queues[key] = [callback];
                fn.apply(null, args.concat([function () {
                    memo[key] = arguments;
                    var q = queues[key];
                    delete queues[key];
                    for (var i = 0, l = q.length; i < l; i++) {
                      q[i].apply(null, arguments);
                    }
                }]));
            }
        };
        memoized.memo = memo;
        memoized.unmemoized = fn;
        return memoized;
    };

    async.unmemoize = function (fn) {
      return function () {
        return (fn.unmemoized || fn).apply(null, arguments);
      };
    };

    async.times = function (count, iterator, callback) {
        var counter = [];
        for (var i = 0; i < count; i++) {
            counter.push(i);
        }
        return async.map(counter, iterator, callback);
    };

    async.timesSeries = function (count, iterator, callback) {
        var counter = [];
        for (var i = 0; i < count; i++) {
            counter.push(i);
        }
        return async.mapSeries(counter, iterator, callback);
    };

    async.seq = function (/* functions... */) {
        var fns = arguments;
        return function () {
            var that = this;
            var args = Array.prototype.slice.call(arguments);
            var callback = args.pop();
            async.reduce(fns, args, function (newargs, fn, cb) {
                fn.apply(that, newargs.concat([function () {
                    var err = arguments[0];
                    var nextargs = Array.prototype.slice.call(arguments, 1);
                    cb(err, nextargs);
                }]))
            },
            function (err, results) {
                callback.apply(that, [err].concat(results));
            });
        };
    };

    async.compose = function (/* functions... */) {
      return async.seq.apply(null, Array.prototype.reverse.call(arguments));
    };

    var _applyEach = function (eachfn, fns /*args...*/) {
        var go = function () {
            var that = this;
            var args = Array.prototype.slice.call(arguments);
            var callback = args.pop();
            return eachfn(fns, function (fn, cb) {
                fn.apply(that, args.concat([cb]));
            },
            callback);
        };
        if (arguments.length > 2) {
            var args = Array.prototype.slice.call(arguments, 2);
            return go.apply(this, args);
        }
        else {
            return go;
        }
    };
    async.applyEach = doParallel(_applyEach);
    async.applyEachSeries = doSeries(_applyEach);

    async.forever = function (fn, callback) {
        function next(err) {
            if (err) {
                if (callback) {
                    return callback(err);
                }
                throw err;
            }
            fn(next);
        }
        next();
    };

    // Node.js
    if (typeof module !== 'undefined' && module.exports) {
        module.exports = async;
    }
    // AMD / RequireJS
    else if (typeof define !== 'undefined' && define.amd) {
        define([], function () {
            return async;
        });
    }
    // included directly via <script> tag
    else {
        root.async = async;
    }

}());

}).call(this,require("JkpR2F"))
},{"JkpR2F":62}],50:[function(require,module,exports){
"use strict";
var Promise = require("./promise/promise").Promise;
var polyfill = require("./promise/polyfill").polyfill;
exports.Promise = Promise;
exports.polyfill = polyfill;
},{"./promise/polyfill":54,"./promise/promise":55}],51:[function(require,module,exports){
"use strict";
/* global toString */

var isArray = require("./utils").isArray;
var isFunction = require("./utils").isFunction;

/**
  Returns a promise that is fulfilled when all the given promises have been
  fulfilled, or rejected if any of them become rejected. The return promise
  is fulfilled with an array that gives all the values in the order they were
  passed in the `promises` array argument.

  Example:

  ```javascript
  var promise1 = RSVP.resolve(1);
  var promise2 = RSVP.resolve(2);
  var promise3 = RSVP.resolve(3);
  var promises = [ promise1, promise2, promise3 ];

  RSVP.all(promises).then(function(array){
    // The array here would be [ 1, 2, 3 ];
  });
  ```

  If any of the `promises` given to `RSVP.all` are rejected, the first promise
  that is rejected will be given as an argument to the returned promises's
  rejection handler. For example:

  Example:

  ```javascript
  var promise1 = RSVP.resolve(1);
  var promise2 = RSVP.reject(new Error("2"));
  var promise3 = RSVP.reject(new Error("3"));
  var promises = [ promise1, promise2, promise3 ];

  RSVP.all(promises).then(function(array){
    // Code here never runs because there are rejected promises!
  }, function(error) {
    // error.message === "2"
  });
  ```

  @method all
  @for RSVP
  @param {Array} promises
  @param {String} label
  @return {Promise} promise that is fulfilled when all `promises` have been
  fulfilled, or rejected if any of them become rejected.
*/
function all(promises) {
  /*jshint validthis:true */
  var Promise = this;

  if (!isArray(promises)) {
    throw new TypeError('You must pass an array to all.');
  }

  return new Promise(function(resolve, reject) {
    var results = [], remaining = promises.length,
    promise;

    if (remaining === 0) {
      resolve([]);
    }

    function resolver(index) {
      return function(value) {
        resolveAll(index, value);
      };
    }

    function resolveAll(index, value) {
      results[index] = value;
      if (--remaining === 0) {
        resolve(results);
      }
    }

    for (var i = 0; i < promises.length; i++) {
      promise = promises[i];

      if (promise && isFunction(promise.then)) {
        promise.then(resolver(i), reject);
      } else {
        resolveAll(i, promise);
      }
    }
  });
}

exports.all = all;
},{"./utils":59}],52:[function(require,module,exports){
(function (process,global){
"use strict";
var browserGlobal = (typeof window !== 'undefined') ? window : {};
var BrowserMutationObserver = browserGlobal.MutationObserver || browserGlobal.WebKitMutationObserver;
var local = (typeof global !== 'undefined') ? global : (this === undefined? window:this);

// node
function useNextTick() {
  return function() {
    process.nextTick(flush);
  };
}

function useMutationObserver() {
  var iterations = 0;
  var observer = new BrowserMutationObserver(flush);
  var node = document.createTextNode('');
  observer.observe(node, { characterData: true });

  return function() {
    node.data = (iterations = ++iterations % 2);
  };
}

function useSetTimeout() {
  return function() {
    local.setTimeout(flush, 1);
  };
}

var queue = [];
function flush() {
  for (var i = 0; i < queue.length; i++) {
    var tuple = queue[i];
    var callback = tuple[0], arg = tuple[1];
    callback(arg);
  }
  queue = [];
}

var scheduleFlush;

// Decide what async method to use to triggering processing of queued callbacks:
if (typeof process !== 'undefined' && {}.toString.call(process) === '[object process]') {
  scheduleFlush = useNextTick();
} else if (BrowserMutationObserver) {
  scheduleFlush = useMutationObserver();
} else {
  scheduleFlush = useSetTimeout();
}

function asap(callback, arg) {
  var length = queue.push([callback, arg]);
  if (length === 1) {
    // If length is 1, that means that we need to schedule an async flush.
    // If additional callbacks are queued before the queue is flushed, they
    // will be processed by this flush that we are scheduling.
    scheduleFlush();
  }
}

exports.asap = asap;
}).call(this,require("JkpR2F"),typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : {})
},{"JkpR2F":62}],53:[function(require,module,exports){
"use strict";
var config = {
  instrument: false
};

function configure(name, value) {
  if (arguments.length === 2) {
    config[name] = value;
  } else {
    return config[name];
  }
}

exports.config = config;
exports.configure = configure;
},{}],54:[function(require,module,exports){
(function (global){
"use strict";
/*global self*/
var RSVPPromise = require("./promise").Promise;
var isFunction = require("./utils").isFunction;

function polyfill() {
  var local;

  if (typeof global !== 'undefined') {
    local = global;
  } else if (typeof window !== 'undefined' && window.document) {
    local = window;
  } else {
    local = self;
  }

  var es6PromiseSupport = 
    "Promise" in local &&
    // Some of these methods are missing from
    // Firefox/Chrome experimental implementations
    "resolve" in local.Promise &&
    "reject" in local.Promise &&
    "all" in local.Promise &&
    "race" in local.Promise &&
    // Older version of the spec had a resolver object
    // as the arg rather than a function
    (function() {
      var resolve;
      new local.Promise(function(r) { resolve = r; });
      return isFunction(resolve);
    }());

  if (!es6PromiseSupport) {
    local.Promise = RSVPPromise;
  }
}

exports.polyfill = polyfill;
}).call(this,typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : {})
},{"./promise":55,"./utils":59}],55:[function(require,module,exports){
"use strict";
var config = require("./config").config;
var configure = require("./config").configure;
var objectOrFunction = require("./utils").objectOrFunction;
var isFunction = require("./utils").isFunction;
var now = require("./utils").now;
var all = require("./all").all;
var race = require("./race").race;
var staticResolve = require("./resolve").resolve;
var staticReject = require("./reject").reject;
var asap = require("./asap").asap;

var counter = 0;

config.async = asap; // default async is asap;

function Promise(resolver) {
  if (!isFunction(resolver)) {
    throw new TypeError('You must pass a resolver function as the first argument to the promise constructor');
  }

  if (!(this instanceof Promise)) {
    throw new TypeError("Failed to construct 'Promise': Please use the 'new' operator, this object constructor cannot be called as a function.");
  }

  this._subscribers = [];

  invokeResolver(resolver, this);
}

function invokeResolver(resolver, promise) {
  function resolvePromise(value) {
    resolve(promise, value);
  }

  function rejectPromise(reason) {
    reject(promise, reason);
  }

  try {
    resolver(resolvePromise, rejectPromise);
  } catch(e) {
    rejectPromise(e);
  }
}

function invokeCallback(settled, promise, callback, detail) {
  var hasCallback = isFunction(callback),
      value, error, succeeded, failed;

  if (hasCallback) {
    try {
      value = callback(detail);
      succeeded = true;
    } catch(e) {
      failed = true;
      error = e;
    }
  } else {
    value = detail;
    succeeded = true;
  }

  if (handleThenable(promise, value)) {
    return;
  } else if (hasCallback && succeeded) {
    resolve(promise, value);
  } else if (failed) {
    reject(promise, error);
  } else if (settled === FULFILLED) {
    resolve(promise, value);
  } else if (settled === REJECTED) {
    reject(promise, value);
  }
}

var PENDING   = void 0;
var SEALED    = 0;
var FULFILLED = 1;
var REJECTED  = 2;

function subscribe(parent, child, onFulfillment, onRejection) {
  var subscribers = parent._subscribers;
  var length = subscribers.length;

  subscribers[length] = child;
  subscribers[length + FULFILLED] = onFulfillment;
  subscribers[length + REJECTED]  = onRejection;
}

function publish(promise, settled) {
  var child, callback, subscribers = promise._subscribers, detail = promise._detail;

  for (var i = 0; i < subscribers.length; i += 3) {
    child = subscribers[i];
    callback = subscribers[i + settled];

    invokeCallback(settled, child, callback, detail);
  }

  promise._subscribers = null;
}

Promise.prototype = {
  constructor: Promise,

  _state: undefined,
  _detail: undefined,
  _subscribers: undefined,

  then: function(onFulfillment, onRejection) {
    var promise = this;

    var thenPromise = new this.constructor(function() {});

    if (this._state) {
      var callbacks = arguments;
      config.async(function invokePromiseCallback() {
        invokeCallback(promise._state, thenPromise, callbacks[promise._state - 1], promise._detail);
      });
    } else {
      subscribe(this, thenPromise, onFulfillment, onRejection);
    }

    return thenPromise;
  },

  'catch': function(onRejection) {
    return this.then(null, onRejection);
  }
};

Promise.all = all;
Promise.race = race;
Promise.resolve = staticResolve;
Promise.reject = staticReject;

function handleThenable(promise, value) {
  var then = null,
  resolved;

  try {
    if (promise === value) {
      throw new TypeError("A promises callback cannot return that same promise.");
    }

    if (objectOrFunction(value)) {
      then = value.then;

      if (isFunction(then)) {
        then.call(value, function(val) {
          if (resolved) { return true; }
          resolved = true;

          if (value !== val) {
            resolve(promise, val);
          } else {
            fulfill(promise, val);
          }
        }, function(val) {
          if (resolved) { return true; }
          resolved = true;

          reject(promise, val);
        });

        return true;
      }
    }
  } catch (error) {
    if (resolved) { return true; }
    reject(promise, error);
    return true;
  }

  return false;
}

function resolve(promise, value) {
  if (promise === value) {
    fulfill(promise, value);
  } else if (!handleThenable(promise, value)) {
    fulfill(promise, value);
  }
}

function fulfill(promise, value) {
  if (promise._state !== PENDING) { return; }
  promise._state = SEALED;
  promise._detail = value;

  config.async(publishFulfillment, promise);
}

function reject(promise, reason) {
  if (promise._state !== PENDING) { return; }
  promise._state = SEALED;
  promise._detail = reason;

  config.async(publishRejection, promise);
}

function publishFulfillment(promise) {
  publish(promise, promise._state = FULFILLED);
}

function publishRejection(promise) {
  publish(promise, promise._state = REJECTED);
}

exports.Promise = Promise;
},{"./all":51,"./asap":52,"./config":53,"./race":56,"./reject":57,"./resolve":58,"./utils":59}],56:[function(require,module,exports){
"use strict";
/* global toString */
var isArray = require("./utils").isArray;

/**
  `RSVP.race` allows you to watch a series of promises and act as soon as the
  first promise given to the `promises` argument fulfills or rejects.

  Example:

  ```javascript
  var promise1 = new RSVP.Promise(function(resolve, reject){
    setTimeout(function(){
      resolve("promise 1");
    }, 200);
  });

  var promise2 = new RSVP.Promise(function(resolve, reject){
    setTimeout(function(){
      resolve("promise 2");
    }, 100);
  });

  RSVP.race([promise1, promise2]).then(function(result){
    // result === "promise 2" because it was resolved before promise1
    // was resolved.
  });
  ```

  `RSVP.race` is deterministic in that only the state of the first completed
  promise matters. For example, even if other promises given to the `promises`
  array argument are resolved, but the first completed promise has become
  rejected before the other promises became fulfilled, the returned promise
  will become rejected:

  ```javascript
  var promise1 = new RSVP.Promise(function(resolve, reject){
    setTimeout(function(){
      resolve("promise 1");
    }, 200);
  });

  var promise2 = new RSVP.Promise(function(resolve, reject){
    setTimeout(function(){
      reject(new Error("promise 2"));
    }, 100);
  });

  RSVP.race([promise1, promise2]).then(function(result){
    // Code here never runs because there are rejected promises!
  }, function(reason){
    // reason.message === "promise2" because promise 2 became rejected before
    // promise 1 became fulfilled
  });
  ```

  @method race
  @for RSVP
  @param {Array} promises array of promises to observe
  @param {String} label optional string for describing the promise returned.
  Useful for tooling.
  @return {Promise} a promise that becomes fulfilled with the value the first
  completed promises is resolved with if the first completed promise was
  fulfilled, or rejected with the reason that the first completed promise
  was rejected with.
*/
function race(promises) {
  /*jshint validthis:true */
  var Promise = this;

  if (!isArray(promises)) {
    throw new TypeError('You must pass an array to race.');
  }
  return new Promise(function(resolve, reject) {
    var results = [], promise;

    for (var i = 0; i < promises.length; i++) {
      promise = promises[i];

      if (promise && typeof promise.then === 'function') {
        promise.then(resolve, reject);
      } else {
        resolve(promise);
      }
    }
  });
}

exports.race = race;
},{"./utils":59}],57:[function(require,module,exports){
"use strict";
/**
  `RSVP.reject` returns a promise that will become rejected with the passed
  `reason`. `RSVP.reject` is essentially shorthand for the following:

  ```javascript
  var promise = new RSVP.Promise(function(resolve, reject){
    reject(new Error('WHOOPS'));
  });

  promise.then(function(value){
    // Code here doesn't run because the promise is rejected!
  }, function(reason){
    // reason.message === 'WHOOPS'
  });
  ```

  Instead of writing the above, your code now simply becomes the following:

  ```javascript
  var promise = RSVP.reject(new Error('WHOOPS'));

  promise.then(function(value){
    // Code here doesn't run because the promise is rejected!
  }, function(reason){
    // reason.message === 'WHOOPS'
  });
  ```

  @method reject
  @for RSVP
  @param {Any} reason value that the returned promise will be rejected with.
  @param {String} label optional string for identifying the returned promise.
  Useful for tooling.
  @return {Promise} a promise that will become rejected with the given
  `reason`.
*/
function reject(reason) {
  /*jshint validthis:true */
  var Promise = this;

  return new Promise(function (resolve, reject) {
    reject(reason);
  });
}

exports.reject = reject;
},{}],58:[function(require,module,exports){
"use strict";
function resolve(value) {
  /*jshint validthis:true */
  if (value && typeof value === 'object' && value.constructor === this) {
    return value;
  }

  var Promise = this;

  return new Promise(function(resolve) {
    resolve(value);
  });
}

exports.resolve = resolve;
},{}],59:[function(require,module,exports){
"use strict";
function objectOrFunction(x) {
  return isFunction(x) || (typeof x === "object" && x !== null);
}

function isFunction(x) {
  return typeof x === "function";
}

function isArray(x) {
  return Object.prototype.toString.call(x) === "[object Array]";
}

// Date.now is not available in browsers < IE9
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/now#Compatibility
var now = Date.now || function() { return new Date().getTime(); };


exports.objectOrFunction = objectOrFunction;
exports.isFunction = isFunction;
exports.isArray = isArray;
exports.now = now;
},{}],60:[function(require,module,exports){
var hasOwn = Object.prototype.hasOwnProperty;
var toString = Object.prototype.toString;

function isPlainObject(obj) {
	if (!obj || toString.call(obj) !== '[object Object]' || obj.nodeType || obj.setInterval)
		return false;

	var has_own_constructor = hasOwn.call(obj, 'constructor');
	var has_is_property_of_method = hasOwn.call(obj.constructor.prototype, 'isPrototypeOf');
	// Not own constructor property must be Object
	if (obj.constructor && !has_own_constructor && !has_is_property_of_method)
		return false;

	// Own properties are enumerated firstly, so to speed up,
	// if last one is own, then all properties are own.
	var key;
	for ( key in obj ) {}

	return key === undefined || hasOwn.call( obj, key );
};

module.exports = function extend() {
	var options, name, src, copy, copyIsArray, clone,
	    target = arguments[0] || {},
	    i = 1,
	    length = arguments.length,
	    deep = false;

	// Handle a deep copy situation
	if ( typeof target === "boolean" ) {
		deep = target;
		target = arguments[1] || {};
		// skip the boolean and the target
		i = 2;
	}

	// Handle case when target is a string or something (possible in deep copy)
	if ( typeof target !== "object" && typeof target !== "function") {
		target = {};
	}

	for ( ; i < length; i++ ) {
		// Only deal with non-null/undefined values
		if ( (options = arguments[ i ]) != null ) {
			// Extend the base object
			for ( name in options ) {
				src = target[ name ];
				copy = options[ name ];

				// Prevent never-ending loop
				if ( target === copy ) {
					continue;
				}

				// Recurse if we're merging plain objects or arrays
				if ( deep && copy && ( isPlainObject(copy) || (copyIsArray = Array.isArray(copy)) ) ) {
					if ( copyIsArray ) {
						copyIsArray = false;
						clone = src && Array.isArray(src) ? src : [];

					} else {
						clone = src && isPlainObject(src) ? src : {};
					}

					// Never move original objects, clone them
					target[ name ] = extend( deep, clone, copy );

				// Don't bring in undefined values
				} else if ( copy !== undefined ) {
					target[ name ] = copy;
				}
			}
		}
	}

	// Return the modified object
	return target;
};

},{}],61:[function(require,module,exports){
// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

function EventEmitter() {
  this._events = this._events || {};
  this._maxListeners = this._maxListeners || undefined;
}
module.exports = EventEmitter;

// Backwards-compat with node 0.10.x
EventEmitter.EventEmitter = EventEmitter;

EventEmitter.prototype._events = undefined;
EventEmitter.prototype._maxListeners = undefined;

// By default EventEmitters will print a warning if more than 10 listeners are
// added to it. This is a useful default which helps finding memory leaks.
EventEmitter.defaultMaxListeners = 10;

// Obviously not all Emitters should be limited to 10. This function allows
// that to be increased. Set to zero for unlimited.
EventEmitter.prototype.setMaxListeners = function(n) {
  if (!isNumber(n) || n < 0 || isNaN(n))
    throw TypeError('n must be a positive number');
  this._maxListeners = n;
  return this;
};

EventEmitter.prototype.emit = function(type) {
  var er, handler, len, args, i, listeners;

  if (!this._events)
    this._events = {};

  // If there is no 'error' event listener then throw.
  if (type === 'error') {
    if (!this._events.error ||
        (isObject(this._events.error) && !this._events.error.length)) {
      er = arguments[1];
      if (er instanceof Error) {
        throw er; // Unhandled 'error' event
      } else {
        throw TypeError('Uncaught, unspecified "error" event.');
      }
      return false;
    }
  }

  handler = this._events[type];

  if (isUndefined(handler))
    return false;

  if (isFunction(handler)) {
    switch (arguments.length) {
      // fast cases
      case 1:
        handler.call(this);
        break;
      case 2:
        handler.call(this, arguments[1]);
        break;
      case 3:
        handler.call(this, arguments[1], arguments[2]);
        break;
      // slower
      default:
        len = arguments.length;
        args = new Array(len - 1);
        for (i = 1; i < len; i++)
          args[i - 1] = arguments[i];
        handler.apply(this, args);
    }
  } else if (isObject(handler)) {
    len = arguments.length;
    args = new Array(len - 1);
    for (i = 1; i < len; i++)
      args[i - 1] = arguments[i];

    listeners = handler.slice();
    len = listeners.length;
    for (i = 0; i < len; i++)
      listeners[i].apply(this, args);
  }

  return true;
};

EventEmitter.prototype.addListener = function(type, listener) {
  var m;

  if (!isFunction(listener))
    throw TypeError('listener must be a function');

  if (!this._events)
    this._events = {};

  // To avoid recursion in the case that type === "newListener"! Before
  // adding it to the listeners, first emit "newListener".
  if (this._events.newListener)
    this.emit('newListener', type,
              isFunction(listener.listener) ?
              listener.listener : listener);

  if (!this._events[type])
    // Optimize the case of one listener. Don't need the extra array object.
    this._events[type] = listener;
  else if (isObject(this._events[type]))
    // If we've already got an array, just append.
    this._events[type].push(listener);
  else
    // Adding the second element, need to change to array.
    this._events[type] = [this._events[type], listener];

  // Check for listener leak
  if (isObject(this._events[type]) && !this._events[type].warned) {
    var m;
    if (!isUndefined(this._maxListeners)) {
      m = this._maxListeners;
    } else {
      m = EventEmitter.defaultMaxListeners;
    }

    if (m && m > 0 && this._events[type].length > m) {
      this._events[type].warned = true;
      console.error('(node) warning: possible EventEmitter memory ' +
                    'leak detected. %d listeners added. ' +
                    'Use emitter.setMaxListeners() to increase limit.',
                    this._events[type].length);
      if (typeof console.trace === 'function') {
        // not supported in IE 10
        console.trace();
      }
    }
  }

  return this;
};

EventEmitter.prototype.on = EventEmitter.prototype.addListener;

EventEmitter.prototype.once = function(type, listener) {
  if (!isFunction(listener))
    throw TypeError('listener must be a function');

  var fired = false;

  function g() {
    this.removeListener(type, g);

    if (!fired) {
      fired = true;
      listener.apply(this, arguments);
    }
  }

  g.listener = listener;
  this.on(type, g);

  return this;
};

// emits a 'removeListener' event iff the listener was removed
EventEmitter.prototype.removeListener = function(type, listener) {
  var list, position, length, i;

  if (!isFunction(listener))
    throw TypeError('listener must be a function');

  if (!this._events || !this._events[type])
    return this;

  list = this._events[type];
  length = list.length;
  position = -1;

  if (list === listener ||
      (isFunction(list.listener) && list.listener === listener)) {
    delete this._events[type];
    if (this._events.removeListener)
      this.emit('removeListener', type, listener);

  } else if (isObject(list)) {
    for (i = length; i-- > 0;) {
      if (list[i] === listener ||
          (list[i].listener && list[i].listener === listener)) {
        position = i;
        break;
      }
    }

    if (position < 0)
      return this;

    if (list.length === 1) {
      list.length = 0;
      delete this._events[type];
    } else {
      list.splice(position, 1);
    }

    if (this._events.removeListener)
      this.emit('removeListener', type, listener);
  }

  return this;
};

EventEmitter.prototype.removeAllListeners = function(type) {
  var key, listeners;

  if (!this._events)
    return this;

  // not listening for removeListener, no need to emit
  if (!this._events.removeListener) {
    if (arguments.length === 0)
      this._events = {};
    else if (this._events[type])
      delete this._events[type];
    return this;
  }

  // emit removeListener for all listeners on all events
  if (arguments.length === 0) {
    for (key in this._events) {
      if (key === 'removeListener') continue;
      this.removeAllListeners(key);
    }
    this.removeAllListeners('removeListener');
    this._events = {};
    return this;
  }

  listeners = this._events[type];

  if (isFunction(listeners)) {
    this.removeListener(type, listeners);
  } else {
    // LIFO order
    while (listeners.length)
      this.removeListener(type, listeners[listeners.length - 1]);
  }
  delete this._events[type];

  return this;
};

EventEmitter.prototype.listeners = function(type) {
  var ret;
  if (!this._events || !this._events[type])
    ret = [];
  else if (isFunction(this._events[type]))
    ret = [this._events[type]];
  else
    ret = this._events[type].slice();
  return ret;
};

EventEmitter.listenerCount = function(emitter, type) {
  var ret;
  if (!emitter._events || !emitter._events[type])
    ret = 0;
  else if (isFunction(emitter._events[type]))
    ret = 1;
  else
    ret = emitter._events[type].length;
  return ret;
};

function isFunction(arg) {
  return typeof arg === 'function';
}

function isNumber(arg) {
  return typeof arg === 'number';
}

function isObject(arg) {
  return typeof arg === 'object' && arg !== null;
}

function isUndefined(arg) {
  return arg === void 0;
}

},{}],62:[function(require,module,exports){
// shim for using process in browser

var process = module.exports = {};

process.nextTick = (function () {
    var canSetImmediate = typeof window !== 'undefined'
    && window.setImmediate;
    var canPost = typeof window !== 'undefined'
    && window.postMessage && window.addEventListener
    ;

    if (canSetImmediate) {
        return function (f) { return window.setImmediate(f) };
    }

    if (canPost) {
        var queue = [];
        window.addEventListener('message', function (ev) {
            var source = ev.source;
            if ((source === window || source === null) && ev.data === 'process-tick') {
                ev.stopPropagation();
                if (queue.length > 0) {
                    var fn = queue.shift();
                    fn();
                }
            }
        }, true);

        return function nextTick(fn) {
            queue.push(fn);
            window.postMessage('process-tick', '*');
        };
    }

    return function nextTick(fn) {
        setTimeout(fn, 0);
    };
})();

process.title = 'browser';
process.browser = true;
process.env = {};
process.argv = [];

function noop() {}

process.on = noop;
process.addListener = noop;
process.once = noop;
process.off = noop;
process.removeListener = noop;
process.removeAllListeners = noop;
process.emit = noop;

process.binding = function (name) {
    throw new Error('process.binding is not supported');
}

// TODO(shtylman)
process.cwd = function () { return '/' };
process.chdir = function (dir) {
    throw new Error('process.chdir is not supported');
};

},{}],63:[function(require,module,exports){
(function (global){
/*! http://mths.be/punycode v1.2.4 by @mathias */
;(function(root) {

	/** Detect free variables */
	var freeExports = typeof exports == 'object' && exports;
	var freeModule = typeof module == 'object' && module &&
		module.exports == freeExports && module;
	var freeGlobal = typeof global == 'object' && global;
	if (freeGlobal.global === freeGlobal || freeGlobal.window === freeGlobal) {
		root = freeGlobal;
	}

	/**
	 * The `punycode` object.
	 * @name punycode
	 * @type Object
	 */
	var punycode,

	/** Highest positive signed 32-bit float value */
	maxInt = 2147483647, // aka. 0x7FFFFFFF or 2^31-1

	/** Bootstring parameters */
	base = 36,
	tMin = 1,
	tMax = 26,
	skew = 38,
	damp = 700,
	initialBias = 72,
	initialN = 128, // 0x80
	delimiter = '-', // '\x2D'

	/** Regular expressions */
	regexPunycode = /^xn--/,
	regexNonASCII = /[^ -~]/, // unprintable ASCII chars + non-ASCII chars
	regexSeparators = /\x2E|\u3002|\uFF0E|\uFF61/g, // RFC 3490 separators

	/** Error messages */
	errors = {
		'overflow': 'Overflow: input needs wider integers to process',
		'not-basic': 'Illegal input >= 0x80 (not a basic code point)',
		'invalid-input': 'Invalid input'
	},

	/** Convenience shortcuts */
	baseMinusTMin = base - tMin,
	floor = Math.floor,
	stringFromCharCode = String.fromCharCode,

	/** Temporary variable */
	key;

	/*--------------------------------------------------------------------------*/

	/**
	 * A generic error utility function.
	 * @private
	 * @param {String} type The error type.
	 * @returns {Error} Throws a `RangeError` with the applicable error message.
	 */
	function error(type) {
		throw RangeError(errors[type]);
	}

	/**
	 * A generic `Array#map` utility function.
	 * @private
	 * @param {Array} array The array to iterate over.
	 * @param {Function} callback The function that gets called for every array
	 * item.
	 * @returns {Array} A new array of values returned by the callback function.
	 */
	function map(array, fn) {
		var length = array.length;
		while (length--) {
			array[length] = fn(array[length]);
		}
		return array;
	}

	/**
	 * A simple `Array#map`-like wrapper to work with domain name strings.
	 * @private
	 * @param {String} domain The domain name.
	 * @param {Function} callback The function that gets called for every
	 * character.
	 * @returns {Array} A new string of characters returned by the callback
	 * function.
	 */
	function mapDomain(string, fn) {
		return map(string.split(regexSeparators), fn).join('.');
	}

	/**
	 * Creates an array containing the numeric code points of each Unicode
	 * character in the string. While JavaScript uses UCS-2 internally,
	 * this function will convert a pair of surrogate halves (each of which
	 * UCS-2 exposes as separate characters) into a single code point,
	 * matching UTF-16.
	 * @see `punycode.ucs2.encode`
	 * @see <http://mathiasbynens.be/notes/javascript-encoding>
	 * @memberOf punycode.ucs2
	 * @name decode
	 * @param {String} string The Unicode input string (UCS-2).
	 * @returns {Array} The new array of code points.
	 */
	function ucs2decode(string) {
		var output = [],
		    counter = 0,
		    length = string.length,
		    value,
		    extra;
		while (counter < length) {
			value = string.charCodeAt(counter++);
			if (value >= 0xD800 && value <= 0xDBFF && counter < length) {
				// high surrogate, and there is a next character
				extra = string.charCodeAt(counter++);
				if ((extra & 0xFC00) == 0xDC00) { // low surrogate
					output.push(((value & 0x3FF) << 10) + (extra & 0x3FF) + 0x10000);
				} else {
					// unmatched surrogate; only append this code unit, in case the next
					// code unit is the high surrogate of a surrogate pair
					output.push(value);
					counter--;
				}
			} else {
				output.push(value);
			}
		}
		return output;
	}

	/**
	 * Creates a string based on an array of numeric code points.
	 * @see `punycode.ucs2.decode`
	 * @memberOf punycode.ucs2
	 * @name encode
	 * @param {Array} codePoints The array of numeric code points.
	 * @returns {String} The new Unicode string (UCS-2).
	 */
	function ucs2encode(array) {
		return map(array, function(value) {
			var output = '';
			if (value > 0xFFFF) {
				value -= 0x10000;
				output += stringFromCharCode(value >>> 10 & 0x3FF | 0xD800);
				value = 0xDC00 | value & 0x3FF;
			}
			output += stringFromCharCode(value);
			return output;
		}).join('');
	}

	/**
	 * Converts a basic code point into a digit/integer.
	 * @see `digitToBasic()`
	 * @private
	 * @param {Number} codePoint The basic numeric code point value.
	 * @returns {Number} The numeric value of a basic code point (for use in
	 * representing integers) in the range `0` to `base - 1`, or `base` if
	 * the code point does not represent a value.
	 */
	function basicToDigit(codePoint) {
		if (codePoint - 48 < 10) {
			return codePoint - 22;
		}
		if (codePoint - 65 < 26) {
			return codePoint - 65;
		}
		if (codePoint - 97 < 26) {
			return codePoint - 97;
		}
		return base;
	}

	/**
	 * Converts a digit/integer into a basic code point.
	 * @see `basicToDigit()`
	 * @private
	 * @param {Number} digit The numeric value of a basic code point.
	 * @returns {Number} The basic code point whose value (when used for
	 * representing integers) is `digit`, which needs to be in the range
	 * `0` to `base - 1`. If `flag` is non-zero, the uppercase form is
	 * used; else, the lowercase form is used. The behavior is undefined
	 * if `flag` is non-zero and `digit` has no uppercase form.
	 */
	function digitToBasic(digit, flag) {
		//  0..25 map to ASCII a..z or A..Z
		// 26..35 map to ASCII 0..9
		return digit + 22 + 75 * (digit < 26) - ((flag != 0) << 5);
	}

	/**
	 * Bias adaptation function as per section 3.4 of RFC 3492.
	 * http://tools.ietf.org/html/rfc3492#section-3.4
	 * @private
	 */
	function adapt(delta, numPoints, firstTime) {
		var k = 0;
		delta = firstTime ? floor(delta / damp) : delta >> 1;
		delta += floor(delta / numPoints);
		for (/* no initialization */; delta > baseMinusTMin * tMax >> 1; k += base) {
			delta = floor(delta / baseMinusTMin);
		}
		return floor(k + (baseMinusTMin + 1) * delta / (delta + skew));
	}

	/**
	 * Converts a Punycode string of ASCII-only symbols to a string of Unicode
	 * symbols.
	 * @memberOf punycode
	 * @param {String} input The Punycode string of ASCII-only symbols.
	 * @returns {String} The resulting string of Unicode symbols.
	 */
	function decode(input) {
		// Don't use UCS-2
		var output = [],
		    inputLength = input.length,
		    out,
		    i = 0,
		    n = initialN,
		    bias = initialBias,
		    basic,
		    j,
		    index,
		    oldi,
		    w,
		    k,
		    digit,
		    t,
		    /** Cached calculation results */
		    baseMinusT;

		// Handle the basic code points: let `basic` be the number of input code
		// points before the last delimiter, or `0` if there is none, then copy
		// the first basic code points to the output.

		basic = input.lastIndexOf(delimiter);
		if (basic < 0) {
			basic = 0;
		}

		for (j = 0; j < basic; ++j) {
			// if it's not a basic code point
			if (input.charCodeAt(j) >= 0x80) {
				error('not-basic');
			}
			output.push(input.charCodeAt(j));
		}

		// Main decoding loop: start just after the last delimiter if any basic code
		// points were copied; start at the beginning otherwise.

		for (index = basic > 0 ? basic + 1 : 0; index < inputLength; /* no final expression */) {

			// `index` is the index of the next character to be consumed.
			// Decode a generalized variable-length integer into `delta`,
			// which gets added to `i`. The overflow checking is easier
			// if we increase `i` as we go, then subtract off its starting
			// value at the end to obtain `delta`.
			for (oldi = i, w = 1, k = base; /* no condition */; k += base) {

				if (index >= inputLength) {
					error('invalid-input');
				}

				digit = basicToDigit(input.charCodeAt(index++));

				if (digit >= base || digit > floor((maxInt - i) / w)) {
					error('overflow');
				}

				i += digit * w;
				t = k <= bias ? tMin : (k >= bias + tMax ? tMax : k - bias);

				if (digit < t) {
					break;
				}

				baseMinusT = base - t;
				if (w > floor(maxInt / baseMinusT)) {
					error('overflow');
				}

				w *= baseMinusT;

			}

			out = output.length + 1;
			bias = adapt(i - oldi, out, oldi == 0);

			// `i` was supposed to wrap around from `out` to `0`,
			// incrementing `n` each time, so we'll fix that now:
			if (floor(i / out) > maxInt - n) {
				error('overflow');
			}

			n += floor(i / out);
			i %= out;

			// Insert `n` at position `i` of the output
			output.splice(i++, 0, n);

		}

		return ucs2encode(output);
	}

	/**
	 * Converts a string of Unicode symbols to a Punycode string of ASCII-only
	 * symbols.
	 * @memberOf punycode
	 * @param {String} input The string of Unicode symbols.
	 * @returns {String} The resulting Punycode string of ASCII-only symbols.
	 */
	function encode(input) {
		var n,
		    delta,
		    handledCPCount,
		    basicLength,
		    bias,
		    j,
		    m,
		    q,
		    k,
		    t,
		    currentValue,
		    output = [],
		    /** `inputLength` will hold the number of code points in `input`. */
		    inputLength,
		    /** Cached calculation results */
		    handledCPCountPlusOne,
		    baseMinusT,
		    qMinusT;

		// Convert the input in UCS-2 to Unicode
		input = ucs2decode(input);

		// Cache the length
		inputLength = input.length;

		// Initialize the state
		n = initialN;
		delta = 0;
		bias = initialBias;

		// Handle the basic code points
		for (j = 0; j < inputLength; ++j) {
			currentValue = input[j];
			if (currentValue < 0x80) {
				output.push(stringFromCharCode(currentValue));
			}
		}

		handledCPCount = basicLength = output.length;

		// `handledCPCount` is the number of code points that have been handled;
		// `basicLength` is the number of basic code points.

		// Finish the basic string - if it is not empty - with a delimiter
		if (basicLength) {
			output.push(delimiter);
		}

		// Main encoding loop:
		while (handledCPCount < inputLength) {

			// All non-basic code points < n have been handled already. Find the next
			// larger one:
			for (m = maxInt, j = 0; j < inputLength; ++j) {
				currentValue = input[j];
				if (currentValue >= n && currentValue < m) {
					m = currentValue;
				}
			}

			// Increase `delta` enough to advance the decoder's <n,i> state to <m,0>,
			// but guard against overflow
			handledCPCountPlusOne = handledCPCount + 1;
			if (m - n > floor((maxInt - delta) / handledCPCountPlusOne)) {
				error('overflow');
			}

			delta += (m - n) * handledCPCountPlusOne;
			n = m;

			for (j = 0; j < inputLength; ++j) {
				currentValue = input[j];

				if (currentValue < n && ++delta > maxInt) {
					error('overflow');
				}

				if (currentValue == n) {
					// Represent delta as a generalized variable-length integer
					for (q = delta, k = base; /* no condition */; k += base) {
						t = k <= bias ? tMin : (k >= bias + tMax ? tMax : k - bias);
						if (q < t) {
							break;
						}
						qMinusT = q - t;
						baseMinusT = base - t;
						output.push(
							stringFromCharCode(digitToBasic(t + qMinusT % baseMinusT, 0))
						);
						q = floor(qMinusT / baseMinusT);
					}

					output.push(stringFromCharCode(digitToBasic(q, 0)));
					bias = adapt(delta, handledCPCountPlusOne, handledCPCount == basicLength);
					delta = 0;
					++handledCPCount;
				}
			}

			++delta;
			++n;

		}
		return output.join('');
	}

	/**
	 * Converts a Punycode string representing a domain name to Unicode. Only the
	 * Punycoded parts of the domain name will be converted, i.e. it doesn't
	 * matter if you call it on a string that has already been converted to
	 * Unicode.
	 * @memberOf punycode
	 * @param {String} domain The Punycode domain name to convert to Unicode.
	 * @returns {String} The Unicode representation of the given Punycode
	 * string.
	 */
	function toUnicode(domain) {
		return mapDomain(domain, function(string) {
			return regexPunycode.test(string)
				? decode(string.slice(4).toLowerCase())
				: string;
		});
	}

	/**
	 * Converts a Unicode string representing a domain name to Punycode. Only the
	 * non-ASCII parts of the domain name will be converted, i.e. it doesn't
	 * matter if you call it with a domain that's already in ASCII.
	 * @memberOf punycode
	 * @param {String} domain The domain name to convert, as a Unicode string.
	 * @returns {String} The Punycode representation of the given domain name.
	 */
	function toASCII(domain) {
		return mapDomain(domain, function(string) {
			return regexNonASCII.test(string)
				? 'xn--' + encode(string)
				: string;
		});
	}

	/*--------------------------------------------------------------------------*/

	/** Define the public API */
	punycode = {
		/**
		 * A string representing the current Punycode.js version number.
		 * @memberOf punycode
		 * @type String
		 */
		'version': '1.2.4',
		/**
		 * An object of methods to convert from JavaScript's internal character
		 * representation (UCS-2) to Unicode code points, and back.
		 * @see <http://mathiasbynens.be/notes/javascript-encoding>
		 * @memberOf punycode
		 * @type Object
		 */
		'ucs2': {
			'decode': ucs2decode,
			'encode': ucs2encode
		},
		'decode': decode,
		'encode': encode,
		'toASCII': toASCII,
		'toUnicode': toUnicode
	};

	/** Expose `punycode` */
	// Some AMD build optimizers, like r.js, check for specific condition patterns
	// like the following:
	if (
		typeof define == 'function' &&
		typeof define.amd == 'object' &&
		define.amd
	) {
		define('punycode', function() {
			return punycode;
		});
	} else if (freeExports && !freeExports.nodeType) {
		if (freeModule) { // in Node.js or RingoJS v0.8.0+
			freeModule.exports = punycode;
		} else { // in Narwhal or RingoJS v0.7.0-
			for (key in punycode) {
				punycode.hasOwnProperty(key) && (freeExports[key] = punycode[key]);
			}
		}
	} else { // in Rhino or a web browser
		root.punycode = punycode;
	}

}(this));

}).call(this,typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : {})
},{}],64:[function(require,module,exports){
// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

'use strict';

// If obj.hasOwnProperty has been overridden, then calling
// obj.hasOwnProperty(prop) will break.
// See: https://github.com/joyent/node/issues/1707
function hasOwnProperty(obj, prop) {
  return Object.prototype.hasOwnProperty.call(obj, prop);
}

module.exports = function(qs, sep, eq, options) {
  sep = sep || '&';
  eq = eq || '=';
  var obj = {};

  if (typeof qs !== 'string' || qs.length === 0) {
    return obj;
  }

  var regexp = /\+/g;
  qs = qs.split(sep);

  var maxKeys = 1000;
  if (options && typeof options.maxKeys === 'number') {
    maxKeys = options.maxKeys;
  }

  var len = qs.length;
  // maxKeys <= 0 means that we should not limit keys count
  if (maxKeys > 0 && len > maxKeys) {
    len = maxKeys;
  }

  for (var i = 0; i < len; ++i) {
    var x = qs[i].replace(regexp, '%20'),
        idx = x.indexOf(eq),
        kstr, vstr, k, v;

    if (idx >= 0) {
      kstr = x.substr(0, idx);
      vstr = x.substr(idx + 1);
    } else {
      kstr = x;
      vstr = '';
    }

    k = decodeURIComponent(kstr);
    v = decodeURIComponent(vstr);

    if (!hasOwnProperty(obj, k)) {
      obj[k] = v;
    } else if (isArray(obj[k])) {
      obj[k].push(v);
    } else {
      obj[k] = [obj[k], v];
    }
  }

  return obj;
};

var isArray = Array.isArray || function (xs) {
  return Object.prototype.toString.call(xs) === '[object Array]';
};

},{}],65:[function(require,module,exports){
// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

'use strict';

var stringifyPrimitive = function(v) {
  switch (typeof v) {
    case 'string':
      return v;

    case 'boolean':
      return v ? 'true' : 'false';

    case 'number':
      return isFinite(v) ? v : '';

    default:
      return '';
  }
};

module.exports = function(obj, sep, eq, name) {
  sep = sep || '&';
  eq = eq || '=';
  if (obj === null) {
    obj = undefined;
  }

  if (typeof obj === 'object') {
    return map(objectKeys(obj), function(k) {
      var ks = encodeURIComponent(stringifyPrimitive(k)) + eq;
      if (isArray(obj[k])) {
        return map(obj[k], function(v) {
          return ks + encodeURIComponent(stringifyPrimitive(v));
        }).join(sep);
      } else {
        return ks + encodeURIComponent(stringifyPrimitive(obj[k]));
      }
    }).join(sep);

  }

  if (!name) return '';
  return encodeURIComponent(stringifyPrimitive(name)) + eq +
         encodeURIComponent(stringifyPrimitive(obj));
};

var isArray = Array.isArray || function (xs) {
  return Object.prototype.toString.call(xs) === '[object Array]';
};

function map (xs, f) {
  if (xs.map) return xs.map(f);
  var res = [];
  for (var i = 0; i < xs.length; i++) {
    res.push(f(xs[i], i));
  }
  return res;
}

var objectKeys = Object.keys || function (obj) {
  var res = [];
  for (var key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) res.push(key);
  }
  return res;
};

},{}],66:[function(require,module,exports){
'use strict';

exports.decode = exports.parse = require('./decode');
exports.encode = exports.stringify = require('./encode');

},{"./decode":64,"./encode":65}],67:[function(require,module,exports){
// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

var punycode = require('punycode');

exports.parse = urlParse;
exports.resolve = urlResolve;
exports.resolveObject = urlResolveObject;
exports.format = urlFormat;

exports.Url = Url;

function Url() {
  this.protocol = null;
  this.slashes = null;
  this.auth = null;
  this.host = null;
  this.port = null;
  this.hostname = null;
  this.hash = null;
  this.search = null;
  this.query = null;
  this.pathname = null;
  this.path = null;
  this.href = null;
}

// Reference: RFC 3986, RFC 1808, RFC 2396

// define these here so at least they only have to be
// compiled once on the first module load.
var protocolPattern = /^([a-z0-9.+-]+:)/i,
    portPattern = /:[0-9]*$/,

    // RFC 2396: characters reserved for delimiting URLs.
    // We actually just auto-escape these.
    delims = ['<', '>', '"', '`', ' ', '\r', '\n', '\t'],

    // RFC 2396: characters not allowed for various reasons.
    unwise = ['{', '}', '|', '\\', '^', '`'].concat(delims),

    // Allowed by RFCs, but cause of XSS attacks.  Always escape these.
    autoEscape = ['\''].concat(unwise),
    // Characters that are never ever allowed in a hostname.
    // Note that any invalid chars are also handled, but these
    // are the ones that are *expected* to be seen, so we fast-path
    // them.
    nonHostChars = ['%', '/', '?', ';', '#'].concat(autoEscape),
    hostEndingChars = ['/', '?', '#'],
    hostnameMaxLen = 255,
    hostnamePartPattern = /^[a-z0-9A-Z_-]{0,63}$/,
    hostnamePartStart = /^([a-z0-9A-Z_-]{0,63})(.*)$/,
    // protocols that can allow "unsafe" and "unwise" chars.
    unsafeProtocol = {
      'javascript': true,
      'javascript:': true
    },
    // protocols that never have a hostname.
    hostlessProtocol = {
      'javascript': true,
      'javascript:': true
    },
    // protocols that always contain a // bit.
    slashedProtocol = {
      'http': true,
      'https': true,
      'ftp': true,
      'gopher': true,
      'file': true,
      'http:': true,
      'https:': true,
      'ftp:': true,
      'gopher:': true,
      'file:': true
    },
    querystring = require('querystring');

function urlParse(url, parseQueryString, slashesDenoteHost) {
  if (url && isObject(url) && url instanceof Url) return url;

  var u = new Url;
  u.parse(url, parseQueryString, slashesDenoteHost);
  return u;
}

Url.prototype.parse = function(url, parseQueryString, slashesDenoteHost) {
  if (!isString(url)) {
    throw new TypeError("Parameter 'url' must be a string, not " + typeof url);
  }

  var rest = url;

  // trim before proceeding.
  // This is to support parse stuff like "  http://foo.com  \n"
  rest = rest.trim();

  var proto = protocolPattern.exec(rest);
  if (proto) {
    proto = proto[0];
    var lowerProto = proto.toLowerCase();
    this.protocol = lowerProto;
    rest = rest.substr(proto.length);
  }

  // figure out if it's got a host
  // user@server is *always* interpreted as a hostname, and url
  // resolution will treat //foo/bar as host=foo,path=bar because that's
  // how the browser resolves relative URLs.
  if (slashesDenoteHost || proto || rest.match(/^\/\/[^@\/]+@[^@\/]+/)) {
    var slashes = rest.substr(0, 2) === '//';
    if (slashes && !(proto && hostlessProtocol[proto])) {
      rest = rest.substr(2);
      this.slashes = true;
    }
  }

  if (!hostlessProtocol[proto] &&
      (slashes || (proto && !slashedProtocol[proto]))) {

    // there's a hostname.
    // the first instance of /, ?, ;, or # ends the host.
    //
    // If there is an @ in the hostname, then non-host chars *are* allowed
    // to the left of the last @ sign, unless some host-ending character
    // comes *before* the @-sign.
    // URLs are obnoxious.
    //
    // ex:
    // http://a@b@c/ => user:a@b host:c
    // http://a@b?@c => user:a host:c path:/?@c

    // v0.12 TODO(isaacs): This is not quite how Chrome does things.
    // Review our test case against browsers more comprehensively.

    // find the first instance of any hostEndingChars
    var hostEnd = -1;
    for (var i = 0; i < hostEndingChars.length; i++) {
      var hec = rest.indexOf(hostEndingChars[i]);
      if (hec !== -1 && (hostEnd === -1 || hec < hostEnd))
        hostEnd = hec;
    }

    // at this point, either we have an explicit point where the
    // auth portion cannot go past, or the last @ char is the decider.
    var auth, atSign;
    if (hostEnd === -1) {
      // atSign can be anywhere.
      atSign = rest.lastIndexOf('@');
    } else {
      // atSign must be in auth portion.
      // http://a@b/c@d => host:b auth:a path:/c@d
      atSign = rest.lastIndexOf('@', hostEnd);
    }

    // Now we have a portion which is definitely the auth.
    // Pull that off.
    if (atSign !== -1) {
      auth = rest.slice(0, atSign);
      rest = rest.slice(atSign + 1);
      this.auth = decodeURIComponent(auth);
    }

    // the host is the remaining to the left of the first non-host char
    hostEnd = -1;
    for (var i = 0; i < nonHostChars.length; i++) {
      var hec = rest.indexOf(nonHostChars[i]);
      if (hec !== -1 && (hostEnd === -1 || hec < hostEnd))
        hostEnd = hec;
    }
    // if we still have not hit it, then the entire thing is a host.
    if (hostEnd === -1)
      hostEnd = rest.length;

    this.host = rest.slice(0, hostEnd);
    rest = rest.slice(hostEnd);

    // pull out port.
    this.parseHost();

    // we've indicated that there is a hostname,
    // so even if it's empty, it has to be present.
    this.hostname = this.hostname || '';

    // if hostname begins with [ and ends with ]
    // assume that it's an IPv6 address.
    var ipv6Hostname = this.hostname[0] === '[' &&
        this.hostname[this.hostname.length - 1] === ']';

    // validate a little.
    if (!ipv6Hostname) {
      var hostparts = this.hostname.split(/\./);
      for (var i = 0, l = hostparts.length; i < l; i++) {
        var part = hostparts[i];
        if (!part) continue;
        if (!part.match(hostnamePartPattern)) {
          var newpart = '';
          for (var j = 0, k = part.length; j < k; j++) {
            if (part.charCodeAt(j) > 127) {
              // we replace non-ASCII char with a temporary placeholder
              // we need this to make sure size of hostname is not
              // broken by replacing non-ASCII by nothing
              newpart += 'x';
            } else {
              newpart += part[j];
            }
          }
          // we test again with ASCII char only
          if (!newpart.match(hostnamePartPattern)) {
            var validParts = hostparts.slice(0, i);
            var notHost = hostparts.slice(i + 1);
            var bit = part.match(hostnamePartStart);
            if (bit) {
              validParts.push(bit[1]);
              notHost.unshift(bit[2]);
            }
            if (notHost.length) {
              rest = '/' + notHost.join('.') + rest;
            }
            this.hostname = validParts.join('.');
            break;
          }
        }
      }
    }

    if (this.hostname.length > hostnameMaxLen) {
      this.hostname = '';
    } else {
      // hostnames are always lower case.
      this.hostname = this.hostname.toLowerCase();
    }

    if (!ipv6Hostname) {
      // IDNA Support: Returns a puny coded representation of "domain".
      // It only converts the part of the domain name that
      // has non ASCII characters. I.e. it dosent matter if
      // you call it with a domain that already is in ASCII.
      var domainArray = this.hostname.split('.');
      var newOut = [];
      for (var i = 0; i < domainArray.length; ++i) {
        var s = domainArray[i];
        newOut.push(s.match(/[^A-Za-z0-9_-]/) ?
            'xn--' + punycode.encode(s) : s);
      }
      this.hostname = newOut.join('.');
    }

    var p = this.port ? ':' + this.port : '';
    var h = this.hostname || '';
    this.host = h + p;
    this.href += this.host;

    // strip [ and ] from the hostname
    // the host field still retains them, though
    if (ipv6Hostname) {
      this.hostname = this.hostname.substr(1, this.hostname.length - 2);
      if (rest[0] !== '/') {
        rest = '/' + rest;
      }
    }
  }

  // now rest is set to the post-host stuff.
  // chop off any delim chars.
  if (!unsafeProtocol[lowerProto]) {

    // First, make 100% sure that any "autoEscape" chars get
    // escaped, even if encodeURIComponent doesn't think they
    // need to be.
    for (var i = 0, l = autoEscape.length; i < l; i++) {
      var ae = autoEscape[i];
      var esc = encodeURIComponent(ae);
      if (esc === ae) {
        esc = escape(ae);
      }
      rest = rest.split(ae).join(esc);
    }
  }


  // chop off from the tail first.
  var hash = rest.indexOf('#');
  if (hash !== -1) {
    // got a fragment string.
    this.hash = rest.substr(hash);
    rest = rest.slice(0, hash);
  }
  var qm = rest.indexOf('?');
  if (qm !== -1) {
    this.search = rest.substr(qm);
    this.query = rest.substr(qm + 1);
    if (parseQueryString) {
      this.query = querystring.parse(this.query);
    }
    rest = rest.slice(0, qm);
  } else if (parseQueryString) {
    // no query string, but parseQueryString still requested
    this.search = '';
    this.query = {};
  }
  if (rest) this.pathname = rest;
  if (slashedProtocol[lowerProto] &&
      this.hostname && !this.pathname) {
    this.pathname = '/';
  }

  //to support http.request
  if (this.pathname || this.search) {
    var p = this.pathname || '';
    var s = this.search || '';
    this.path = p + s;
  }

  // finally, reconstruct the href based on what has been validated.
  this.href = this.format();
  return this;
};

// format a parsed object into a url string
function urlFormat(obj) {
  // ensure it's an object, and not a string url.
  // If it's an obj, this is a no-op.
  // this way, you can call url_format() on strings
  // to clean up potentially wonky urls.
  if (isString(obj)) obj = urlParse(obj);
  if (!(obj instanceof Url)) return Url.prototype.format.call(obj);
  return obj.format();
}

Url.prototype.format = function() {
  var auth = this.auth || '';
  if (auth) {
    auth = encodeURIComponent(auth);
    auth = auth.replace(/%3A/i, ':');
    auth += '@';
  }

  var protocol = this.protocol || '',
      pathname = this.pathname || '',
      hash = this.hash || '',
      host = false,
      query = '';

  if (this.host) {
    host = auth + this.host;
  } else if (this.hostname) {
    host = auth + (this.hostname.indexOf(':') === -1 ?
        this.hostname :
        '[' + this.hostname + ']');
    if (this.port) {
      host += ':' + this.port;
    }
  }

  if (this.query &&
      isObject(this.query) &&
      Object.keys(this.query).length) {
    query = querystring.stringify(this.query);
  }

  var search = this.search || (query && ('?' + query)) || '';

  if (protocol && protocol.substr(-1) !== ':') protocol += ':';

  // only the slashedProtocols get the //.  Not mailto:, xmpp:, etc.
  // unless they had them to begin with.
  if (this.slashes ||
      (!protocol || slashedProtocol[protocol]) && host !== false) {
    host = '//' + (host || '');
    if (pathname && pathname.charAt(0) !== '/') pathname = '/' + pathname;
  } else if (!host) {
    host = '';
  }

  if (hash && hash.charAt(0) !== '#') hash = '#' + hash;
  if (search && search.charAt(0) !== '?') search = '?' + search;

  pathname = pathname.replace(/[?#]/g, function(match) {
    return encodeURIComponent(match);
  });
  search = search.replace('#', '%23');

  return protocol + host + pathname + search + hash;
};

function urlResolve(source, relative) {
  return urlParse(source, false, true).resolve(relative);
}

Url.prototype.resolve = function(relative) {
  return this.resolveObject(urlParse(relative, false, true)).format();
};

function urlResolveObject(source, relative) {
  if (!source) return relative;
  return urlParse(source, false, true).resolveObject(relative);
}

Url.prototype.resolveObject = function(relative) {
  if (isString(relative)) {
    var rel = new Url();
    rel.parse(relative, false, true);
    relative = rel;
  }

  var result = new Url();
  Object.keys(this).forEach(function(k) {
    result[k] = this[k];
  }, this);

  // hash is always overridden, no matter what.
  // even href="" will remove it.
  result.hash = relative.hash;

  // if the relative url is empty, then there's nothing left to do here.
  if (relative.href === '') {
    result.href = result.format();
    return result;
  }

  // hrefs like //foo/bar always cut to the protocol.
  if (relative.slashes && !relative.protocol) {
    // take everything except the protocol from relative
    Object.keys(relative).forEach(function(k) {
      if (k !== 'protocol')
        result[k] = relative[k];
    });

    //urlParse appends trailing / to urls like http://www.example.com
    if (slashedProtocol[result.protocol] &&
        result.hostname && !result.pathname) {
      result.path = result.pathname = '/';
    }

    result.href = result.format();
    return result;
  }

  if (relative.protocol && relative.protocol !== result.protocol) {
    // if it's a known url protocol, then changing
    // the protocol does weird things
    // first, if it's not file:, then we MUST have a host,
    // and if there was a path
    // to begin with, then we MUST have a path.
    // if it is file:, then the host is dropped,
    // because that's known to be hostless.
    // anything else is assumed to be absolute.
    if (!slashedProtocol[relative.protocol]) {
      Object.keys(relative).forEach(function(k) {
        result[k] = relative[k];
      });
      result.href = result.format();
      return result;
    }

    result.protocol = relative.protocol;
    if (!relative.host && !hostlessProtocol[relative.protocol]) {
      var relPath = (relative.pathname || '').split('/');
      while (relPath.length && !(relative.host = relPath.shift()));
      if (!relative.host) relative.host = '';
      if (!relative.hostname) relative.hostname = '';
      if (relPath[0] !== '') relPath.unshift('');
      if (relPath.length < 2) relPath.unshift('');
      result.pathname = relPath.join('/');
    } else {
      result.pathname = relative.pathname;
    }
    result.search = relative.search;
    result.query = relative.query;
    result.host = relative.host || '';
    result.auth = relative.auth;
    result.hostname = relative.hostname || relative.host;
    result.port = relative.port;
    // to support http.request
    if (result.pathname || result.search) {
      var p = result.pathname || '';
      var s = result.search || '';
      result.path = p + s;
    }
    result.slashes = result.slashes || relative.slashes;
    result.href = result.format();
    return result;
  }

  var isSourceAbs = (result.pathname && result.pathname.charAt(0) === '/'),
      isRelAbs = (
          relative.host ||
          relative.pathname && relative.pathname.charAt(0) === '/'
      ),
      mustEndAbs = (isRelAbs || isSourceAbs ||
                    (result.host && relative.pathname)),
      removeAllDots = mustEndAbs,
      srcPath = result.pathname && result.pathname.split('/') || [],
      relPath = relative.pathname && relative.pathname.split('/') || [],
      psychotic = result.protocol && !slashedProtocol[result.protocol];

  // if the url is a non-slashed url, then relative
  // links like ../.. should be able
  // to crawl up to the hostname, as well.  This is strange.
  // result.protocol has already been set by now.
  // Later on, put the first path part into the host field.
  if (psychotic) {
    result.hostname = '';
    result.port = null;
    if (result.host) {
      if (srcPath[0] === '') srcPath[0] = result.host;
      else srcPath.unshift(result.host);
    }
    result.host = '';
    if (relative.protocol) {
      relative.hostname = null;
      relative.port = null;
      if (relative.host) {
        if (relPath[0] === '') relPath[0] = relative.host;
        else relPath.unshift(relative.host);
      }
      relative.host = null;
    }
    mustEndAbs = mustEndAbs && (relPath[0] === '' || srcPath[0] === '');
  }

  if (isRelAbs) {
    // it's absolute.
    result.host = (relative.host || relative.host === '') ?
                  relative.host : result.host;
    result.hostname = (relative.hostname || relative.hostname === '') ?
                      relative.hostname : result.hostname;
    result.search = relative.search;
    result.query = relative.query;
    srcPath = relPath;
    // fall through to the dot-handling below.
  } else if (relPath.length) {
    // it's relative
    // throw away the existing file, and take the new path instead.
    if (!srcPath) srcPath = [];
    srcPath.pop();
    srcPath = srcPath.concat(relPath);
    result.search = relative.search;
    result.query = relative.query;
  } else if (!isNullOrUndefined(relative.search)) {
    // just pull out the search.
    // like href='?foo'.
    // Put this after the other two cases because it simplifies the booleans
    if (psychotic) {
      result.hostname = result.host = srcPath.shift();
      //occationaly the auth can get stuck only in host
      //this especialy happens in cases like
      //url.resolveObject('mailto:local1@domain1', 'local2@domain2')
      var authInHost = result.host && result.host.indexOf('@') > 0 ?
                       result.host.split('@') : false;
      if (authInHost) {
        result.auth = authInHost.shift();
        result.host = result.hostname = authInHost.shift();
      }
    }
    result.search = relative.search;
    result.query = relative.query;
    //to support http.request
    if (!isNull(result.pathname) || !isNull(result.search)) {
      result.path = (result.pathname ? result.pathname : '') +
                    (result.search ? result.search : '');
    }
    result.href = result.format();
    return result;
  }

  if (!srcPath.length) {
    // no path at all.  easy.
    // we've already handled the other stuff above.
    result.pathname = null;
    //to support http.request
    if (result.search) {
      result.path = '/' + result.search;
    } else {
      result.path = null;
    }
    result.href = result.format();
    return result;
  }

  // if a url ENDs in . or .., then it must get a trailing slash.
  // however, if it ends in anything else non-slashy,
  // then it must NOT get a trailing slash.
  var last = srcPath.slice(-1)[0];
  var hasTrailingSlash = (
      (result.host || relative.host) && (last === '.' || last === '..') ||
      last === '');

  // strip single dots, resolve double dots to parent dir
  // if the path tries to go above the root, `up` ends up > 0
  var up = 0;
  for (var i = srcPath.length; i >= 0; i--) {
    last = srcPath[i];
    if (last == '.') {
      srcPath.splice(i, 1);
    } else if (last === '..') {
      srcPath.splice(i, 1);
      up++;
    } else if (up) {
      srcPath.splice(i, 1);
      up--;
    }
  }

  // if the path is allowed to go above the root, restore leading ..s
  if (!mustEndAbs && !removeAllDots) {
    for (; up--; up) {
      srcPath.unshift('..');
    }
  }

  if (mustEndAbs && srcPath[0] !== '' &&
      (!srcPath[0] || srcPath[0].charAt(0) !== '/')) {
    srcPath.unshift('');
  }

  if (hasTrailingSlash && (srcPath.join('/').substr(-1) !== '/')) {
    srcPath.push('');
  }

  var isAbsolute = srcPath[0] === '' ||
      (srcPath[0] && srcPath[0].charAt(0) === '/');

  // put the host back
  if (psychotic) {
    result.hostname = result.host = isAbsolute ? '' :
                                    srcPath.length ? srcPath.shift() : '';
    //occationaly the auth can get stuck only in host
    //this especialy happens in cases like
    //url.resolveObject('mailto:local1@domain1', 'local2@domain2')
    var authInHost = result.host && result.host.indexOf('@') > 0 ?
                     result.host.split('@') : false;
    if (authInHost) {
      result.auth = authInHost.shift();
      result.host = result.hostname = authInHost.shift();
    }
  }

  mustEndAbs = mustEndAbs || (result.host && srcPath.length);

  if (mustEndAbs && !isAbsolute) {
    srcPath.unshift('');
  }

  if (!srcPath.length) {
    result.pathname = null;
    result.path = null;
  } else {
    result.pathname = srcPath.join('/');
  }

  //to support request.http
  if (!isNull(result.pathname) || !isNull(result.search)) {
    result.path = (result.pathname ? result.pathname : '') +
                  (result.search ? result.search : '');
  }
  result.auth = relative.auth || result.auth;
  result.slashes = result.slashes || relative.slashes;
  result.href = result.format();
  return result;
};

Url.prototype.parseHost = function() {
  var host = this.host;
  var port = portPattern.exec(host);
  if (port) {
    port = port[0];
    if (port !== ':') {
      this.port = port.substr(1);
    }
    host = host.substr(0, host.length - port.length);
  }
  if (host) this.hostname = host;
};

function isString(arg) {
  return typeof arg === "string";
}

function isObject(arg) {
  return typeof arg === 'object' && arg !== null;
}

function isNull(arg) {
  return arg === null;
}
function isNullOrUndefined(arg) {
  return  arg == null;
}

},{"punycode":63,"querystring":66}],68:[function(require,module,exports){
if (typeof Object.create === 'function') {
  // implementation from standard node.js 'util' module
  module.exports = function inherits(ctor, superCtor) {
    ctor.super_ = superCtor
    ctor.prototype = Object.create(superCtor.prototype, {
      constructor: {
        value: ctor,
        enumerable: false,
        writable: true,
        configurable: true
      }
    });
  };
} else {
  // old school shim for old browsers
  module.exports = function inherits(ctor, superCtor) {
    ctor.super_ = superCtor
    var TempCtor = function () {}
    TempCtor.prototype = superCtor.prototype
    ctor.prototype = new TempCtor()
    ctor.prototype.constructor = ctor
  }
}

},{}],69:[function(require,module,exports){
function Mapper()
{
  var sources = {};


  this.forEach = function(callback)
  {
    for(var key in sources)
    {
      var source = sources[key];

      for(var key2 in source)
        callback(source[key2]);
    };
  };

  this.get = function(id, source)
  {
    var ids = sources[source];
    if(ids == undefined)
      return undefined;

    return ids[id];
  };

  this.remove = function(id, source)
  {
    var ids = sources[source];
    if(ids == undefined)
      return;

    delete ids[id];

    if(!Object.keys(ids).length)
      delete sources[source];
  };

  this.set = function(value, id, source)
  {
    if(value == undefined)
      return this.remove(id, source);

    var ids = sources[source];
    if(ids == undefined)
      sources[source] = ids = {};

    ids[id] = value;
  };
};


Mapper.prototype.pop = function(id, source)
{
  var value = this.get(id, source);
  if(value == undefined)
    return undefined;

  this.remove(id, source);

  return value;
};


module.exports = Mapper;

},{}],70:[function(require,module,exports){
/*
 * (C) Copyright 2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var JsonRpcClient  = require('./jsonrpcclient');


exports.JsonRpcClient  = JsonRpcClient;

},{"./jsonrpcclient":71}],71:[function(require,module,exports){
/*
 * (C) Copyright 2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var RpcBuilder = require('../..');

var WebSocket = require('ws');


function JsonRpcClient(wsUrl, onRequest, onerror)
{
  var ws = new WebSocket(wsUrl);
      ws.addEventListener('error', onerror);

  var rpc = new RpcBuilder(RpcBuilder.packers.JsonRPC, ws, onRequest);

  this.close       = rpc.close.bind(rpc);
  this.sendRequest = rpc.encode.bind(rpc);
};


module.exports  = JsonRpcClient;

},{"../..":72,"ws":76}],72:[function(require,module,exports){
/*
 * (C) Copyright 2014 Kurento (http://kurento.org/)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var EventEmitter = require('events').EventEmitter;

var inherits = require('inherits');

var packers = require('./packers');
var Mapper = require('./Mapper');


const BASE_TIMEOUT = 5000;


function unifyResponseMethods(responseMethods)
{
  if(!responseMethods) return {};

  for(var key in responseMethods)
  {
    var value = responseMethods[key];

    if(typeof value == 'string')
      responseMethods[key] =
      {
        response: value
      }
  };

  return responseMethods;
};

function unifyTransport(transport)
{
  if(!transport) return;

  if(transport instanceof Function)
    return transport;

  if(transport.send instanceof Function)
    return transport.send.bind(transport);

  if(transport.postMessage instanceof Function)
    return transport.postMessage.bind(transport);

  // Transports that only can receive messages, but not send
  if(transport.onmessage !== undefined) return;

  throw new SyntaxError("Transport is not a function nor a valid object");
};


/**
 * Representation of a RPC notification
 *
 * @class
 *
 * @constructor
 *
 * @param {String} method -method of the notification
 * @param params - parameters of the notification
 */
function RpcNotification(method, params)
{
  Object.defineProperty(this, 'method', {value: method, enumerable: true});
  Object.defineProperty(this, 'params', {value: params, enumerable: true});
};


/**
 * @class
 *
 * @constructor
 */
function RpcBuilder(packer, options, transport, onRequest)
{
  var self = this;

  if(!packer)
    throw new SyntaxError('Packer is not defined');

  if(!packer.pack || !packer.unpack)
    throw new SyntaxError('Packer is invalid');

  var responseMethods = unifyResponseMethods(packer.responseMethods);


  if(options instanceof Function || options && options.send instanceof Function)
  {
    if(transport && !(transport instanceof Function))
      throw new SyntaxError("Only a function can be after transport");

    onRequest = transport;
    transport = options;
    options   = undefined;
  };

  if(transport instanceof Function
  || transport && transport.send instanceof Function)
    if(onRequest && !(onRequest instanceof Function))
      throw new SyntaxError("Only a function can be after transport");

  options = options || {};


  EventEmitter.call(this);

  if(onRequest)
    this.on('request', onRequest);


  Object.defineProperty(this, 'peerID', {value: options.peerID});

  var max_retries = options.max_retries || 0;


  function transportMessage(event)
  {
    var message = self.decode(event.data);
    if(message)
      self.emit('request', message);
  };

  Object.defineProperty(this, 'transport',
  {
    get: function()
    {
      return transport;
    },

    set: function(value)
    {
      // Remove listener from old transport
      if(transport && transport.onmessage !== undefined)
        transport.removeEventListener('message', transportMessage);

      // Set listener on new transport
      if(value && value.onmessage !== undefined)
        value.addEventListener('message', transportMessage);

      transport = unifyTransport(value);
    }
  })

  this.transport = transport;


  const request_timeout    = options.request_timeout    || BASE_TIMEOUT;
  const response_timeout   = options.response_timeout   || BASE_TIMEOUT;
  const duplicates_timeout = options.duplicates_timeout || BASE_TIMEOUT;


  var requestID = 0;

  var requests  = new Mapper();
  var responses = new Mapper();
  var processedResponses = new Mapper();

  var message2Key = {};


  /**
   * Store the response to prevent to process duplicate request later
   */
  function storeResponse(message, id, dest)
  {
    var response =
    {
      message: message,
      /** Timeout to auto-clean old responses */
      timeout: setTimeout(function()
      {
        responses.remove(id, dest);
      },
      response_timeout)
    };

    responses.set(response, id, dest);
  };

  /**
   * Store the response to ignore duplicated messages later
   */
  function storeProcessedResponse(ack, from)
  {
    var timeout = setTimeout(function()
    {
      processedResponses.remove(ack, from);
    },
    duplicates_timeout);

    processedResponses.set(timeout, ack, from);
  };


  /**
   * Representation of a RPC request
   *
   * @class
   * @extends RpcNotification
   *
   * @constructor
   *
   * @param {String} method -method of the notification
   * @param params - parameters of the notification
   * @param {Integer} id - identifier of the request
   * @param [from] - source of the notification
   */
  function RpcRequest(method, params, id, from, transport)
  {
    RpcNotification.call(this, method, params);

    Object.defineProperty(this, 'transport',
    {
      get: function()
      {
        return transport;
      },

      set: function(value)
      {
        transport = unifyTransport(value);
      }
    })

    var response = responses.get(id, from);

    /**
     * @constant {Boolean} duplicated
     */
    if(!(transport || self.transport))
      Object.defineProperty(this, 'duplicated',
      {
        value: Boolean(response)
      });

    var responseMethod = responseMethods[method];

    this.pack = function()
    {
      return packer.pack(this, id);
    }

    /**
     * Generate a response to this request
     *
     * @param {Error} [error]
     * @param {*} [result]
     *
     * @returns {string}
     */
    this.reply = function(error, result, transport)
    {
      // Fix optional parameters
      if(error instanceof Function || error && error.send instanceof Function)
      {
        if(result != undefined)
          throw new SyntaxError("There can't be parameters after callback");

        transport = error;
        result = null;
        error = undefined;
      }

      else if(result instanceof Function
      || result && result.send instanceof Function)
      {
        if(transport != undefined)
          throw new SyntaxError("There can't be parameters after callback");

        transport = result;
        result = null;
      };

      transport = unifyTransport(transport);

      // Duplicated request, remove old response timeout
      if(response)
        clearTimeout(response.timeout);

      if(from != undefined)
      {
        if(error)
          error.dest = from;

        if(result)
          result.dest = from;
      };

      var message;

      // New request or overriden one, create new response with provided data
      if(error || result != undefined)
      {
        if(self.peerID != undefined)
        {
          if(error)
            error.from = self.peerID;
          else
            result.from = self.peerID;
        }

        // Protocol indicates that responses has own request methods
        if(responseMethod)
        {
          if(responseMethod.error == undefined && error)
            message =
            {
              error: error
            };

          else
          {
            var method = error
                       ? responseMethod.error
                       : responseMethod.response;

            message =
            {
              method: method,
              params: error || result
            };
          }
        }
        else
          message =
          {
            error:  error,
            result: result
          };

        message = packer.pack(message, id);
      }

      // Duplicate & not-overriden request, re-send old response
      else if(response)
        message = response.message;

      // New empty reply, response null value
      else
        message = packer.pack({result: null}, id);

      // Store the response to prevent to process a duplicated request later
      storeResponse(message, id, from);

      // Return the stored response so it can be directly send back
      transport = transport || this.transport || self.transport;

      if(transport)
        return transport(message);

      return message;
    }
  };
  inherits(RpcRequest, RpcNotification);


  function cancel(message)
  {
    var key = message2Key[message];
    if(!key) return;

    delete message2Key[message];

    var request = requests.pop(key.id, key.dest);
    if(!request) return;

    clearTimeout(request.timeout);

    // Start duplicated responses timeout
    storeProcessedResponse(key.id, key.dest);
  };

  /**
   * Allow to cancel a request and don't wait for a response
   *
   * If `message` is not given, cancel all the request
   */
  this.cancel = function(message)
  {
    if(message) return cancel(message);

    for(var message in message2Key)
      cancel(message);
  };


  this.close = function()
  {
    // Prevent to receive new messages
    var transport = self.transport;
    if(transport && transport.close)
       transport.close();

    // Request & processed responses
    this.cancel();

    processedResponses.forEach(function(timeout)
    {
      clearTimeout(timeout);
    });

    // Responses
    responses.forEach(function(response)
    {
      clearTimeout(response.timeout);
    });
  };


  /**
   * Generates and encode a JsonRPC 2.0 message
   *
   * @param {String} method -method of the notification
   * @param params - parameters of the notification
   * @param [dest] - destination of the notification
   * @param {object} [transport] - transport where to send the message
   * @param [callback] - function called when a response to this request is
   *   received. If not defined, a notification will be send instead
   *
   * @returns {string} A raw JsonRPC 2.0 request or notification string
   */
  this.encode = function(method, params, dest, transport, callback)
  {
    // Fix optional parameters
    if(params instanceof Function)
    {
      if(dest != undefined)
        throw new SyntaxError("There can't be parameters after callback");

      callback  = params;
      transport = undefined;
      dest      = undefined;
      params    = undefined;
    }

    else if(dest instanceof Function)
    {
      if(transport != undefined)
        throw new SyntaxError("There can't be parameters after callback");

      callback  = dest;
      transport = undefined;
      dest      = undefined;
    }

    else if(transport instanceof Function)
    {
      if(callback != undefined)
        throw new SyntaxError("There can't be parameters after callback");

      callback  = transport;
      transport = undefined;
    };

    if(self.peerID != undefined)
    {
      params = params || {};

      params.from = self.peerID;
    };

    if(dest != undefined)
    {
      params = params || {};

      params.dest = dest;
    };

    // Encode message
    var message =
    {
      method: method,
      params: params
    };

    if(callback)
    {
      var id = requestID++;
      var retried = 0;

      message = packer.pack(message, id);

      function dispatchCallback(error, result)
      {
        self.cancel(message);

        callback(error, result);
      };
      
      var request =
      {
        message:         message,
        callback:        dispatchCallback,
        responseMethods: responseMethods[method] || {}
      };

      var encode_transport = unifyTransport(transport);

      function sendRequest(transport)
      {
        request.timeout = setTimeout(timeout,
                                     request_timeout*Math.pow(2, retried++));
        message2Key[message] = {id: id, dest: dest};
        requests.set(request, id, dest);

        transport = transport || encode_transport || self.transport;
        if(transport)
          return transport(message);

        return message;
      };

      function retry(transport)
      {
        transport = unifyTransport(transport);

        console.warn(retried+' retry for request message:',message);

        var timeout = processedResponses.pop(id, dest);
        clearTimeout(timeout);

        return sendRequest(transport);
      };

      function timeout()
      {
        if(retried < max_retries)
          return retry(transport);

        var error = new Error('Request has timed out');
            error.request = message;

        error.retry = retry;

        dispatchCallback(error)
      };

      return sendRequest(transport);
    };

    // Return the packed message
    message = packer.pack(message);

    transport = transport || self.transport;
    if(transport)
      return transport(message);

    return message;
  };

  /**
   * Decode and process a JsonRPC 2.0 message
   *
   * @param {string} message - string with the content of the message
   *
   * @returns {RpcNotification|RpcRequest|undefined} - the representation of the
   *   notification or the request. If a response was processed, it will return
   *   `undefined` to notify that it was processed
   *
   * @throws {TypeError} - Message is not defined
   */
  this.decode = function(message, transport)
  {
    if(!message)
      throw new TypeError("Message is not defined");

    try
    {
      message = packer.unpack(message);
    }
    catch(e)
    {
      // Ignore invalid messages
      return console.debug(e, message);
    };

    var id     = message.id;
    var ack    = message.ack;
    var method = message.method;
    var params = message.params || {};

    var from = params.from;
    var dest = params.dest;

    // Ignore messages send by us
    if(self.peerID != undefined && from == self.peerID) return;

    // Notification
    if(id == undefined && ack == undefined)
      return new RpcNotification(method, params);


    function processRequest()
    {
      // If we have a transport and it's a duplicated request, reply inmediatly
      transport = unifyTransport(transport) || self.transport;
      if(transport)
      {
        var response = responses.get(id, from);
        if(response)
          return transport(response.message);
      };

      var idAck = (id != undefined) ? id : ack;
      return new RpcRequest(method, params, idAck, from, transport);
    };

    function processResponse(request, error, result)
    {
      request.callback(error, result);
    };

    function duplicatedResponse(timeout)
    {
      console.warn("Response already processed", message);

      // Update duplicated responses timeout
      clearTimeout(timeout);
      storeProcessedResponse(ack, from);
    };


    // Request, or response with own method
    if(method)
    {
      // Check if it's a response with own method
      if(dest == undefined || dest == self.peerID)
      {
        var request = requests.get(ack, from);
        if(request)
        {
          var responseMethods = request.responseMethods;

          if(method == responseMethods.error)
            return processResponse(request, params);

          if(method == responseMethods.response)
            return processResponse(request, null, params);

          return processRequest();
        }

        var processed = processedResponses.get(ack, from);
        if(processed)
          return duplicatedResponse(processed);
      }

      // Request
      return processRequest();
    };

    var error  = message.error;
    var result = message.result;

    // Ignore responses not send to us
    if(error  && error.dest  && error.dest  != self.sessionID) return;
    if(result && result.dest && result.dest != self.sessionID) return;

    // Response
    var request = requests.get(ack, from);
    if(!request)
    {
      var processed = processedResponses.get(ack, from);
      if(processed)
        return duplicatedResponse(processed);

      return console.warn("No callback was defined for this message", message);
    };

    // Process response
    processResponse(request, error, result);
  };
};
inherits(RpcBuilder, EventEmitter);


RpcBuilder.RpcNotification = RpcNotification;


module.exports = RpcBuilder;

var clients = require('./clients');

RpcBuilder.clients = clients;
RpcBuilder.packers = packers;

},{"./Mapper":69,"./clients":70,"./packers":75,"events":61,"inherits":68}],73:[function(require,module,exports){
/**
 * JsonRPC 2.0 packer
 */

/**
 * Pack a JsonRPC 2.0 message
 *
 * @param {Object} message - object to be packaged. It requires to have all the
 *   fields needed by the JsonRPC 2.0 message that it's going to be generated
 *
 * @return {String} - the stringified JsonRPC 2.0 message
 */
function pack(message, id)
{
  var result =
  {
    jsonrpc: "2.0"
  };

  // Request
  if(message.method)
  {
    result.method = message.method;

    if(message.params)
      result.params = message.params;

    // Request is a notification
    if(id != undefined)
      result.id = id;
  }

  // Response
  else if(id != undefined)
  {
    if(message.error)
    {
      if(message.result !== undefined)
        throw new TypeError("Both result and error are defined");

      result.error = message.error;
    }
    else if(message.result !== undefined)
      result.result = message.result;
    else
      throw new TypeError("No result or error is defined");

    result.id = id;
  };

  return JSON.stringify(result);
};

/**
 * Unpack a JsonRPC 2.0 message
 *
 * @param {String} message - string with the content of the JsonRPC 2.0 message
 *
 * @throws {TypeError} - Invalid JsonRPC version
 *
 * @return {Object} - object filled with the JsonRPC 2.0 message content
 */
function unpack(message)
{
  var result = message;

  if(typeof message == 'string' || message instanceof String)
    result = JSON.parse(message);

  // Check if it's a valid message

  var version = result.jsonrpc;
  if(version != "2.0")
    throw new TypeError("Invalid JsonRPC version '"+version+"': "+message);

  // Response
  if(result.method == undefined)
  {
    if(result.id == undefined)
      throw new TypeError("Invalid message: "+message);

    var result_defined = result.result !== undefined;
    var error_defined  = result.error  !== undefined;

    // Check only result or error is defined, not both or none
    if(result_defined && error_defined)
      throw new TypeError("Both result and error are defined: "+message);

    if(!result_defined && !error_defined)
      throw new TypeError("No result or error is defined: "+message);

    result.ack = result.id;
    delete result.id;
  }

  // Return unpacked message
  return result;
};


exports.pack   = pack;
exports.unpack = unpack;

},{}],74:[function(require,module,exports){
function pack(message)
{
  throw new TypeError("Not yet implemented");
};

function unpack(message)
{
  throw new TypeError("Not yet implemented");
};


exports.pack   = pack;
exports.unpack = unpack;

},{}],75:[function(require,module,exports){
var JsonRPC = require('./JsonRPC');
var XmlRPC  = require('./XmlRPC');


exports.JsonRPC = JsonRPC;
exports.XmlRPC  = XmlRPC;

},{"./JsonRPC":73,"./XmlRPC":74}],76:[function(require,module,exports){

/**
 * Module dependencies.
 */

var global = (function() { return this; })();

/**
 * WebSocket constructor.
 */

var WebSocket = global.WebSocket || global.MozWebSocket;

/**
 * Module exports.
 */

module.exports = WebSocket ? ws : null;

/**
 * WebSocket constructor.
 *
 * The third `opts` options object gets ignored in web browsers, since it's
 * non-standard, and throws a TypeError if passed to the constructor.
 * See: https://github.com/einaros/ws/issues/227
 *
 * @param {String} uri
 * @param {Array} protocols (optional)
 * @param {Object) opts (optional)
 * @api public
 */

function ws(uri, protocols, opts) {
  var instance;
  if (protocols) {
    instance = new WebSocket(uri, protocols);
  } else {
    instance = new WebSocket(uri);
  }
  return instance;
}

if (WebSocket) ws.prototype = WebSocket.prototype;

},{}]},{},[1]);