Mongowatch
==========

Exposes all data modifications from MongoDB.


In order to use this you must:

* Have access to the oplog. This will not be available on shared DB hosting, as it would reveal everyone else's database transactions to you.
* Have replication enabled. This can be done by starting mongod with the option '--replSet someArbitraryName'. You must then call rs.initiate() from the mongo CLI.
