var EventEmitter = require('events').EventEmitter,
    mongodb = require('mongodb'),
    Server = mongodb.Server,
    Db = mongodb.Db;

var optsDefaults = {
    port: 27017,
    host: 'localhost',
    dbOpts: { w: 1 },
    formatter: function (data) {
        return data;
    },
    onError: function (error) {
        return console.log('Error - MongoWatch:', error);
    },
    onDebug: function () {
    }
};

function fnEmpty(){}

function merge(target) {
    var sources = [].slice.call(arguments, 1);
    sources.forEach(function (source) {
        for (var prop in source) {
            if (target.hasOwnProperty(prop) || source.hasOwnProperty(prop)) {
                target[prop] = source[prop];
            }
        }
    });
    return target;
}

module.exports = MongoWatch;

function MongoWatch(opts) {

    var self = this;

    this.options = merge(optsDefaults, opts);

    this.debug = this.options.onDebug || function () {};

    if (!this.options.host) {
        this.debug("APPLYING DEFAULT CONFIG FOR MONGO-WATCH");
    }

    this.lastOplogTime = opts.lastOplogTime || (opts.lastUpdate ? self.getTimestamp(opts.lastUpdate) : null);

    this.channel = new EventEmitter;
    this.channel.on('error', this.options.onError);
    this.channel.on('connected', function () {
        return self.status = 'connected';
    });

}

MongoWatch.prototype.status = 'connecting';

MongoWatch.prototype.watching = [];

/**
 * Initializes watcher instance
 * @param doneCallback
 */
MongoWatch.prototype.initialize = function (doneCallback) {

    var self = this;

    self.getOplogStream(this.options, function (err, stream, oplogClient) {

        self.stream = stream;

        self.oplogClient = oplogClient;

        if (err) {
            self.channel.emit('error', err);
            doneCallback(err);
            return;
        }

        self.debug("Emiting 'connected'. Stream exists:", self.stream != null);

        if (doneCallback) doneCallback.call(self, null);

        return self.channel.emit('connected');

    });

};

/**
 * Executes callback when watcher is ready
 * @param done
 * @returns {*}
 */
MongoWatch.prototype.ready = function (done) {

    var isReady;

    isReady = this.status === 'connected';

    this.debug('Ready:', isReady);

    if (isReady) {
        return done();
    } else {
        return this.channel.once('connected', done);
    }

};

/**
 * Setup watching collection (if collection
 * @param collectionName
 * @param notifyCallback
 * @returns {*}
 */
MongoWatch.prototype.watch = function (collectionName, notifyCallback) {

    var self = this;

    collectionName || (collectionName = 'all');
    notifyCallback || (notifyCallback = fnEmpty);

    return this.ready(function () {

        if (self.watching[collectionName] == null) {

            var watcher = function (data) {

                var channel, event, formatter, relevant;
                relevant = (collectionName === 'all') || (data.ns === collectionName);

                self.debug('Data changed:', {
                    data: data,
                    watching: collectionName,
                    relevant: relevant
                });

                if (!relevant) {
                    return;
                }

                channel = collectionName ? "change:" + collectionName : 'change';
                //formatter = formats[self.options.format] || formats['raw'];
                //formatter(data);
                event = self.options.formatter ? self.options.formatter(data) : data;

                self.debug('Emitting event:', {
                    channel: channel,
                    event: event
                });

                return self.channel.emit(collectionName, event);
            };

            self.debug('Adding emitter for:', {
                collection: collectionName
            });

            self.stream.on('data', watcher);

            self.watching[collectionName] = watcher;

        }

        self.debug('Adding listener on:', {
            collection: collectionName
        });

        return self.channel.on(collectionName, notifyCallback);
    });
};

MongoWatch.prototype.stop = function (collection) {
    this.debug('Removing listeners for:', collection);
    collection || (collection = 'all');
    this.channel.removeAllListeners(collection);
    this.stream.removeListener('data', this.watching[collection]);
    return delete this.watching[collection];
};

MongoWatch.prototype.stopAll = function () {
    var coll, _results;
    _results = [];
    for (coll in this.watching) {
        _results.push(this.stop(coll));
    }
    return _results;
};

MongoWatch.prototype.getTimestamp = function (date) {
    var time;
    date || (date = new Date());
    time = Math.floor(date.getTime() / 1000);
    return new mongodb.Timestamp(0, time);
};

MongoWatch.prototype.getDate = function (timestamp) {
    return new Date(timestamp.high_ * 1000);
};

MongoWatch.prototype.connect = function (opts, done) {
    var client = new Db(opts.db, new Server(opts.host, opts.port, {
        native_parser: true
    }), opts.dbOpts);

    return client.open(function (err) {
        if (err) {
            return done(err);
        }
        if ((opts.username != null) || (opts.password != null)) {
            return client.authenticate(opts.username, opts.password, function (err, result) {
                return done(err, client);
            });
        } else {
            return done(err, client);
        }
    });
};

MongoWatch.prototype.getOplogStream = function (opts, doneCallback) {

    var self = this;

    return self.connect({
        db: 'local',
        host: opts.host,
        port: opts.port,
        dbOpts: opts.dbOpts,
        username: opts.username,
        password: opts.password
    }, function (err, oplogClient) {

        if (err) {
            return doneCallback(new Error("Error connecting to database: " + err));
        }

        return oplogClient.collection('oplog.rs', function (err, oplog) {
            var connOpts;
            if (err) {
                return doneCallback(err);
            }

            connOpts = {
                tailable: true,
                awaitdata: true,
                oplogReplay: true,
                numberOfRetries: -1
            };

            return oplog.find({}, {
                ts: 1
            }).sort({
                    $natural: -1
                }).limit(1).toArray(function (err, data) {

                    var cursor, lastOplogTime, stream, timeQuery, lastChange;
                    if (self.lastOplogTime) {
                        lastOplogTime = self.lastOplogTime;
                    }
                    else if(data != null && data[0] != null) {
                        lastOplogTime = data[0].ts;
                    }

                    if (!lastOplogTime) {
                        lastOplogTime = self.getTimestamp();
                    }

                    timeQuery = {
                        $gt: lastOplogTime
                    };

                    cursor = oplog.find({
                        ts: timeQuery
                    }, connOpts);
                    stream = cursor.stream();
                    return doneCallback(null, stream, oplogClient);

                });
        });
    });
};

