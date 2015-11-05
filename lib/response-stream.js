/**
 * Created by yeiniel on 05/11/15.
 */

'use strict';

var Readable = require('stream').Readable;
var util = require('util');

util.inherits(ResponseStream, Readable);

/** Query response streamer.
 *
 * @param {Object} client Cassandra client.
 * @param {Object} query The query string.
 * @param {Array} params The parameter list.
 * @param {Object} options The Cassandra client options.
 * @param {Object} streamOptions The options used to build this stream object.
 * @constructor
 */
function ResponseStream(client, query, params, options, streamOptions){
    if(streamOptions === null || streamOptions === undefined){
        streamOptions = {};
    }

    if(!(this instanceof ResponseStream)){
        return new ResponseStream(streamOptions);
    }

    // force object mode
    streamOptions.objectMode = true;

    Readable.call(this, streamOptions);

    // store the cassandra client and op arguments for latter use
    this._client = client;
    this._query = query;
    this._params = params;
    this._options = options;

    // create the input queue
    this._recordQueue = [];
    this._recordQueueLowWatterMark = 250;

    var self = this;

    this._ended = false;
    this.on("end", function(){
        self._ended = true;
    });

    this._options.pageState = null;
}

/** Try to push all records on queue downstream.
 *
 * @private
 */
ResponseStream.prototype._pushRecordQueue = function(){
    // check if stream is ended
    if(!this._ended){
        var length = this._recordQueue.length;
        for(var i = 0; i < length; i++){
            if(!this.push(this._recordQueue.shift())) break;
        }
    }
};

/** Refill the record queue.
 *
 * @private
 */
ResponseStream.prototype._refill = function(){
    var self = this;
    this._client.execute(this._query, this._params, this._options, function(err, results){
        if(err !== null && err !== undefined){
            self.emit("error", err);
            self.push(null);
        }else{
            self._recordQueue.push.apply(self._recordQueue, results.rows);

            // update page state
            self._options.pageState = results.meta.pageState;

            // try to push records downstream
            self._pushRecordQueue();
        }
    });
};

ResponseStream.prototype._read = function(){
    var self = this;

    this._pushRecordQueue();

    // check if queue size is below the low watter mark
    if(this._recordQueue.length < this._recordQueueLowWatterMark){
        // schedule queue refill
        setImmediate(function(){
            ResponseStream.prototype._refill.apply(self);
        });
    }
};

module.exports = ResponseStream;