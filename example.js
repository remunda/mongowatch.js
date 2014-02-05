/**
 * Created by Jan Remunda on 5.2.14.
 */


var MongoWatch = require('./lib/mongowatch.js');

var hourAgo = new Date();
hourAgo.setHours(hourAgo.getHours() - 1);

var options = {
    port: 27017,
    host: 'localhost',
    formatter: function (data) {
        return data;
    },
    onDebug: console.log,
    lastUpdate: hourAgo
};

var watcher = new MongoWatch(options);

watcher.watch('all', function (data) {
    console.log(data.o._id);
});

watcher.initialize(function (err) {
    if (err) {
        throw err;
    }
});

