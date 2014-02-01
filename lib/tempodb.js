var http = require('http');
var https = require('https');
var zlib = require('zlib');
var session = require('./session.js')
var ID = 'TempoDB: ';
var q = require('q');
q.longStackSupport = true;

var TempoDBClient = exports.TempoDBClient =
    function(key, secret, options) {
        /*
            options
                hostname (string)
                port (Integer)
                secure (Boolean)
                version (string)
								maxconns (Integer)
        */
			options = options || {};
			this.session = new session.Session(key, secret, options)
    }

TempoDBClient.prototype.createSeries = function(key, callback) {
    data = {};

    if (typeof key == 'string' && key) {
        data.key = key;
    }

    return this.session.doRequest('POST', '/series/', null, data, callback);
}

TempoDBClient.prototype.getSeries = function(options, callback) {
    /*
        options
            key (Array of keys or single key)
            tag (string or Array[string])
            attr ({key: val, key2: val2})
						limit (Integer)

    */
    options = options || {};
		options.limit = options.limit || 5000;
		//last argument indicates this is a cursored request
    return this.session.doRequest('GET', '/series/segment/', options, null, callback, true);
}

TempoDBClient.prototype.deleteSeries = function(options, callback) {
    /*
        options
            key (Array of keys or single key)
            tag (string or Array[string])
            attr ({key: val, key2: val2})
            allow_truncation (Boolean)

    */
    options = options || {};
    return this.session.doRequest('DELETE', '/series/', options, null, callback);
}

TempoDBClient.prototype.updateSeries = function(seriesKey, name, attributes, tags, callback) {
    if (!(tags instanceof Array)) {
        throw ID + 'tags must be an array';
    }

    if (!(attributes instanceof Object)) {
        throw ID + 'attributes must be an Object';
    }

    data = {
        key: seriesKey,
        name: name,
        attributes: attributes,
        tags: tags
    }

    return this.session.doRequest('PUT', '/series/key/' + encodeURIComponent(series_key) + '/', null, data, callback);
}

/*TempoDBClient.prototype.read = function(start, end, options, callback) {
    /*
        options
            key (Array of keys or single key)
            interval (string)
            function (string)

    */
/*
    options = options || {};
    options.start = session.ISODateString(start);
    options.end = session.ISODateString(end);

    return this.session.doRequest('GET', '/data/segment/', options, null, callback, true);
};*/

TempoDBClient.prototype.read = function(seriesKey, start, end, options, callback) {
    /*
        options
            interval (string)
            function (string)
						limit (integer)

    */
    options = options || {};
    options.start = session.ISODateString(start);
    options.end = session.ISODateString(end);
		options.limit = options.limit || 5000;

    return this.session.doRequest('GET', '/series/key/' + encodeURIComponent(seriesKey) + '/segment', 
				options, null, callback, true);
}


TempoDBClient.prototype.singleValueByKey = function(seriesKey, ts, options, callback) {
  options = options || {};
  options.ts = session.ISODateString(ts);

  return this.session.doRequest('GET', '/series/key/' + encodeURIComponent(seriesKey) + '/single/', options, null, callback);
}

TempoDBClient.prototype.singleValue = function(ts, options, callback) {
    /*
        options
            direction (Specify direction to search in)
            key (Array of keys or single key)
            tag (Array of tags)
            attr (Object of attributes)

    */
    options = options || {};
    options.ts = session.ISODateString(ts);

    return this.session.doRequest('GET', '/single/', options, null, callback);
};

TempoDBClient.prototype.writeKey = function(seriesKey, data, callback) {
    return this.session.doRequest('POST', '/series/key/' + encodeURIComponent(seriesKey) + '/data/', 
				null, data, callback);
}

TempoDBClient.prototype.writeBulk = function(ts, data, callback) {
    var body = {
        t: session.ISODateString(ts),
        data: data
    }

    return this.session.doRequest('POST', '/data/', null, body, callback);
}

TempoDBClient.prototype.writeMulti = function(data, callback) {
    return this.session.doRequest('POST', '/multi/', null, data, callback)
}

/* TempoDBClient.prototype.incrementMulti = function(data, callback) {
    return this.session.doRequest('POST', '/multi/increment/', null, data, callback)
}

TempoDBClient.prototype.incrementKey = function(seriesKey, data, callback) {
    return this.session.doRequest('POST', '/series/key/' + seriesKey + '/increment/', null, data, callback);
}

TempoDBClient.prototype.incrementBulk = function(ts, data, callback) {
    var body = {
        t: session.ISODateString(ts),
        data: data
    }

    return this.session.doRequest('POST', '/increment/', null, body, callback);
} */

TempoDBClient.prototype.deleteKey = function(seriesKey, start, end, callback) {
  var options = {
    start: session.ISODateString(start),
    end:   session.ISODateString(end)
  }

  return this.session.doRequest('DELETE', '/series/key/'+encodeURIComponent(seriesKey)+'/data/', 
			options, null, callback);
}


