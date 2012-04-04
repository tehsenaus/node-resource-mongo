
var promise = require("node-promise");
var coop = require("coop");
var db = require('mongodb');
var async = promise.execute;

module.exports = function (resource) {

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
			this.name = name;
			this.collection = async.call(host.db, host.db.collection, name);
		},

		list: function (context, query, selector) {
			console.log("list", query, selector);
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
			var me = this;
			console.log("update", query, data);
			return this.collection.then(function (collection) {
				return async.call(collection, collection.update, query, resource.serialize(data), {
					safe: true, multi: false, upsert: me.options.upsert
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
			if(!MongoDbCollection.isinstance(this.parentCollection) && !MongoDbChildCollection.isinstance(this.parentCollection)) {
				throw "MongoDbChildCollection " + name + ": parent is not Mongo DB collection";
			}
		},

		list: function (context, query) {
			var select = {}, me = this;
			select[this.name] = 1;
			return this.parentCollection.list(context, context.modelQuery, select).then(function (parent) {
				if(parent instanceof Array) parent = parent[0];
				return parent && parent[me.name] || [];
			});
		},

		create: function (context, data) {
			var _data = {};
			if(data.$push) {
				var k = Object.keys(data.$push)[0];
				_data[this.name + '.$.' + k] = data.$push[k];
			} else {	
				_data[this.name] = resource.serialize(data);
			}
			_data = {
				$push: _data
			};
			return this.parentCollection.update(context, context.modelQuery, _data);
		},

		update: function (context, query, data) {
			if(data.$push) {
				return this.create(context, data);
			}
			throw "not implemented";
	 	}
	});


	// Sets up and registers a MongoDB data store
	function init(options) {
		var host = init.host = new MongoDbHost(options);
		var mongo = function (resource, dataStoreName) {
			return new MongoDbCollection(host, resource, dataStoreName);
		}
		var child = function (resource, dataStoreName) {
			return new MongoDbChildCollection(host, resource, dataStoreName);
		}

		return {
			mongo: mongo, 'mongo-child': child,
			document: mongo, 'document-child': child
		}
	}

	init.MongoDbCollection = MongoDbCollection;
	init.MongoDbChildCollection = MongoDbChildCollection;
	
	return init;
}

