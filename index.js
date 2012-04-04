
var resource = require("synergy/server/resource");
var promise = require("node-promise");
var coop = require("coop");
var db = require('mongodb');
var async = promise.execute;

var MongoDbHost = new coop.Class({
	initialize: function (options) {
		var dbInstance = this.db= new db.Db(options.name,
				new db.Server(options.host, options.port, {auto_reconnect: true}, {}));
  		this.dbp = promise.execute.call(this.db, this.db.open).then(function () {
  			return async.call(dbInstance, dbInstance.authenticate, options.user, options.password);
  		});
	}
});

var MongoDbCollection = resource.Resource.derived({
	initialize: function (host, resource, name) {
		this.host = host;
		this.collection = async.call(host.db, host.db.collection, name);
	},

	list: function (context, query, selector) {
		return this.collection.then(function (collection) {
			args = [query];
			if(selector) args.push(selector);
			var qs = collection.find.apply(collection, args);
			return async.call(qs, qs.toArray);
		});
	},

	create: function (context, data) {
		return this.collection.then(function (collection) {
			return async.call(collection, collection.insert, [resource.serialize(data)]).then(function () {
				return data;
			});
		});
	},

	update: function (context, query, data) {
		return this.collection.then(function (collection) {
			return async.call(collection, collection.update, query, resource.serialize(data), {
				safe: true, multi: false, upsert: false
			}).then(function () {
				return data;
			});
		});
	}
});

var MongoDbChildCollection = resource.Resource.derived({
	initialize: function(host, resource, name) {
		this.parentCollection = resource.parentModelClass.prototype.resource.dataStore;
		this.name = name;
		if(!MongoDbCollection.isinstance(this.parentCollection)) {
			throw new "MongoDbChildCollection " + name + ": parent is not Mongo DB collection";
		}
	},

	list: function (context, query) {
		var select = {};
		select[this.name] = 1;
		return this.parentCollection.list(context, context.modelQuery, select);
	},

	create: function (context, data) {
		var _data = {};
		_data[this.name] = data.$push || resource.serialize(data);
		_data = {
			$push: _data
		};
		return this.parentCollection.update(context, context.modelQuery, _data);
	}
});



// Sets up and registers a MongoDB data store
module.exports = function (options) {
	var host = new MongoDbHost(options);
	var mongo = resource.Resource.serverDataStores.mongo = function (resource, dataStoreName) {
		return new MongoDbCollection(host, resource, dataStoreName);
	}
	var child = resource.Resource.serverDataStores['mongo-child'] = function (resource, dataStoreName) {
		return new MongoDbChildCollection(host, resource, dataStoreName);
	}

	resource.Resource.serverDataStores.document = mongo;
	resource.Resource.serverDataStores['document-child'] = child;
}
