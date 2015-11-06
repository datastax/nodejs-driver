
'use strict';

var Readable = require('stream').Readable;
var util = require('util');

util.inherits(ResponseStream, Readable);

/** Response stream constructor.
 *
 * @param client Cassandra client.
 * @param {String} query SELECT query whose results are going to be streamed.
 * @param {Array} params Array of query parameters.
 * @param {Object} options Options map.
 * @constructor
 */
function ResponseStream(client, query, params, options)
{
    var self = this;

    // provide default value for the Stream base class constructor
    options.stream = options.stream || {};

    // force object mode on this stream
    options.stream.objectMode = true;

    Readable.call(this, options.stream);

    // store the cassandra client and execute() arguments for latter use
    this._client = client;
    this._query = query;
    this._params = params;
    this._options = options;

    // create the internal queue for buffering execute() calls
    this._rows = [];

    // provide default value for the low water mark on the internal queue
    if(options.watermark === null || options.watermark === undefined){
        options.watermark = 250;
    }

    // provide default value for the fetchSize option
    if(options.fetchSize === null || options.fetchSize === undefined){
        options.fetchSize = 5000;
    }

    // create the fetching flag to avoid duplicate calls to execute()
    this._fetching = false;

    // create the more flag to signal if there is more pages to fetch
    this._more = true;

    // create the end flag to signal once a null valua has been push
    this._end = false;

    // catch the end event to update the _end flag
    // this signal the end to avoid any in-flight query to push a row
    this.on("end", function(){ self._end = true; });

    // start fetching rows
    this._fetch();
}

/** Push as many rows as possible downstream.
 *
 * @private
 */
ResponseStream.prototype._pushRows = function()
{
    while(this._rows.length > 0){
        if(this._end || !this.push(this._rows.shift())) break;
    }

    // check the end condition
    if(!this._more && this._rows.length === 0){
        return this.push(null);
    }
};

/** Fetch records.
 *
 * @private
 */
ResponseStream.prototype._fetch = function()
{
    var self = this;

    // signal fetching to avoid duplicate call to execute()
    this._fetching = true;

    this._client.execute(this._query, this._params, this._options,
        function(err, results){
            if(err !== null && err !== undefined) {
                // if an error occur during request notify it downstream
                self.emit("error", err);
            }else{
                // abort if no results
                if(results === null || results === undefined) return self.push(null);

                // move result rows to the internal queue
                self._rows.push.apply(self._rows, results.rows);

                // update page state
                self._options.pageState = results.meta.pageState;

                // check if there is more pages to fetch
                if(results.rows.length < self._options.fetchSize){
                    self._more = false;
                }

                // re enable fetching
                self._fetching = false;

                // push as many rows as possible if any
                self._pushRows();
            }
        }
    );
};

/** Implement the _read() abstract method of the stream.Readable class.
 *
 * @override
 * @private
 */
ResponseStream.prototype._read = function()
{
    // push as many rows as possible if any
    this._pushRows();

    // if the internal row queue level is below the watermark request more
    // results from cassandra
    if(!this._fetching && this._rows.length < this._options.watermark){
        this._fetch();
    }
};

module.exports = ResponseStream;