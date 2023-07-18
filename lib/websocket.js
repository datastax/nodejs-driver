/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const { EventEmitter } = require('events');
const { WebSocket } = require('ws');

/**
 * WebSocketWrapper is a wrapper on the `ws.Websocket` which implements
 * `net.Socket` interface to be used by the `cassandra.Connection`
 */
class WebSocketWrapper extends EventEmitter {
  /**
   * Creates a websocket wrapper instance. To connect use `connect` method
   * @param {object} options client options for a websocket
   */
  constructor(options) {
    super();
    this.options = options;
  }

  /**
   * Creates an instance of a websocket and connects
   * @param {String} port
   * @param {String} address
   * @param {() => void} connectionCallback is called when connection is successfully established
   * @returns {WebSocketWrapper} wrapper itself
   */
  connect(port, address, connectionCallback) {
    const schema = this.options.transport.toLowerCase() === 'securewebsocket' ? 'wss' : 'ws';

    this.ws = new WebSocket(schema+'://'+address+':'+port, this.options.protocols, this.options);
    
    if (connectionCallback) {
      this.ws.on('open', connectionCallback);
    }

    const stream = WebSocket.createWebSocketStream(this.ws, this.options);

    stream.on('error', err => {
      this.emit('error', err);
    });
    stream.on('drain', () => {
      this.emit('drain');
    });
    stream.on('close', () => {
      this.emit('close');
    });
    stream.on('end', () => {
      this.emit('end');
    });

    this.write = stream.write.bind(stream);
    this.pipe = stream.pipe.bind(stream);
    this.end = stream.end.bind(stream);
    this.destroy = stream.destroy.bind(stream);

    return this;
  }

  /**
   * It is not implemented because `ws` lib doesn't provide API to work with
   */
  setTimeout() {}

  /**
   * It is not implemented because `ws` lib doesn't provide API to work with
   */
  setKeepAlive() {}

  /**
   * It is not implemented because `ws` lib doesn't provide API to work with
   */
  setNoDelay() {}
}

module.exports.WebSocketWrapper = WebSocketWrapper;
