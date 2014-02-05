
var EventEmitter = require('events').EventEmitter,
    mongodb = require('mongodb'),
    Server = mongodb.Server,
    Db = mongodb.Db,
    merge = require('node.extend');

var optsDefaults = {
    port: 27017,
    host: 'localhost',
    dbOpts: { w: 1 },    
    formatter: function (data) { return data; },
    onError: function (error) {
        return console.log('Error - MongoConnector:', (error != null ? error.stack : void 0) || error);
    },
    onDebug: function () {
    }
};

module.exports = function MongoConnector(opts) {

    var self = this;

    //apply default options
    this.options = merge(true, optsDefaults, opts);

    if (!this.options.host) {
        console.log("APPLYING DEFAULT CONFIG FOR MONGO-WATCH");
    }

    this.debug = this.options.onDebug || function () { };

    //apply default options END

    this.lastOplogTime = opts.lastOplogTime || (opts.lastUpdate ? self.getTimestamp(opts.lastUpdate) : null);

    this.channel = new EventEmitter;
    this.channel.on('error', this.options.onError);
    this.channel.on('connected', function () {
        return self.status = 'connected';
    });


}

var exports = module.exports;

exports.prototype.status = 'connecting';

exports.prototype.watching = [];

exports.prototype.initialize = function (lastOplogTime, doneCallback) {

    var self = this;

    if (lastOplogTime) {
        this.lastOplogTime = lastOplogTime;
    }

    self.getOplogStream(this.options, function (err, stream, oplogClient) {

        self.stream = stream;

        self.oplogClient = oplogClient;

        if (err) {
            self.channel.emit('error', err);
            doneCallback(err);
            return;
        }

        self.debug("Emiting 'connected'. Stream exists:", self.stream != null);

        doneCallback(self);

        return self.channel.emit('connected');

    });

};

exports.prototype.ready = function (done) {

    var isReady;

    isReady = this.status === 'connected';

    this.debug('Ready:', isReady);

    if (isReady) {
        return done();
    } else {
        return this.channel.once('connected', done);
    }

};

exports.prototype.watch = function (collection, notify) {

    var self = this;

    collection || (collection = 'all');
    notify || (notify = console.log);

    return this.ready(function () {

        if (self.watching[collection] == null) {

            var watcher = function (data) {

                var channel, event, formatter, relevant;
                relevant = (collection === 'all') || (data.ns === collection);

                self.debug('Data changed:', {
                    data: data,
                    watching: collection,
                    relevant: relevant
                });

                if (!relevant) {
                    return;
                }

                channel = collection ? "change:" + collection : 'change';
                //formatter = formats[self.options.format] || formats['raw'];
                //formatter(data);
                event = self.options.formatter ? self.options.formatter(data) : data;

                self.debug('Emitting event:', {
                    channel: channel,
                    event: event
                });

                return self.channel.emit(collection, event);
            };

            self.debug('Adding emitter for:', {
                collection: collection
            });

            self.stream.on('data', watcher);

            self.watching[collection] = watcher;

        }

        self.debug('Adding listener on:', {
            collection: collection
        });

        return self.channel.on(collection, notify);
    });
};

exports.prototype.stop = function (collection) {
    this.debug('Removing listeners for:', collection);
    collection || (collection = 'all');
    this.channel.removeAllListeners(collection);
    this.stream.removeListener('data', this.watching[collection]);
    return delete this.watching[collection];
};

exports.prototype.stopAll = function () {
    var coll, _results;
    _results = [];
    for (coll in this.watching) {
        _results.push(this.stop(coll));
    }
    return _results;
};

exports.prototype.getTimestamp = function (date) {
    var time;
    date || (date = new Date());
    time = Math.floor(date.getTime() / 1000);
    return new mongodb.Timestamp(0, time);
};

exports.prototype.getDate = function (timestamp) {
    return new Date(timestamp.high_ * 1000);
};

exports.prototype.connect = function (opts, done) {
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

exports.prototype.getOplogStream = function (opts, doneCallback) {

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
                else {
                    lastOplogTime = data != null ? (lastChange = data[0]) != null ? lastChange.ts : void 0 : void 0;
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

