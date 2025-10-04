const { info, logAndThrowError } = require('../log');
const { MongoClient, ServerApiVersion } = require('mongodb');

////////////////////////////////////////////////////////////////////////////////
/**
 * Check if a database exists in the MongoDB instance.
 *
 * @async
 * @param {Object} mongo - Mongo connection object containing client and dbName properties
 * @param {MongoClient} mongo.client - Connected MongoDB client instance
 * @param {string} mongo.dbName - Name of the database to check for existence
 * @returns {Promise<void>} Resolves if database exists
 * @throws {Error} Throws error if database does not exist
 */
const dbExists = async function (mongo) {
	let res = await mongo.client.db().admin().listDatabases();
	for (let i = 0; i < res.databases.length; i++) {
		if (res.databases[i].name === mongo.dbName) {
			info(`Database "${mongo.dbName}" exists.`);
			return;
		}
	}
	throw new Error(`Database "${mongo.dbName}" does not exist.`);
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Connect to a Mongo instance
 *  Mongo object definition:
 *  		const mongo = {
 *  			client: null,
 *  			db: null,
 *  			url: process.env.MONGOURL,
 *  			dbName: process.env.MONGODB,
 *  		};
 *
 * @async
 * @param {Object} mongo - Object containing client, db, url and dbName properties
 * @param {MongoClient|null} mongo.client - MongoDB client instance (initially null)
 * @param {Db|null} mongo.db - MongoDB database instance (initially null)
 * @param {string} mongo.url - MongoDB connection URL
 * @param {string} mongo.dbName - Database name to connect to
 * @returns {Promise<Object>} The mongo object with client and db properties set
 * @throws {Error} Throws error if connection fails
 */
const connectToMongo = async function (mongo) {
	try {
		mongo.client = new MongoClient(mongo.url, {
			serverApi: {
				version: ServerApiVersion.v1,
				strict: true,
				deprecationErrors: true,
			},
		});
		await mongo.client.connect();
		await mongo.client.db('admin').command({ ping: 1 });
		info(`Pinged your deployment. You successfully connected to MongoDB!`);
		await dbExists(mongo);
		mongo.db = await mongo.client.db(mongo.dbName);
		return mongo;
	} catch (e) {
		logAndThrowError(`Could not connect to Mongo: ${e.message}`);
	}
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Close connection to a Mongo instance
 *  Mongo object definition:
 *  		const mongo = {
 *  			client: null,
 *  			db: null,
 *  			url: process.env.MONGOURL,
 *  			dbName: process.env.MONGODB,
 *  		};
 *
 * @async
 * @param {Object} mongo - Object containing client, db, url and dbName properties
 * @param {MongoClient} mongo.client - Connected MongoDB client instance
 * @param {Db} mongo.db - Connected MongoDB database instance
 * @returns {Promise<Object>} The mongo object with client and db properties set to null
 */
const closeMongo = async function (mongo) {
	try {
		await mongo.client.close();
		info('Closed Mongodb connection.');
		mongo.client = null;
		mongo.db = null;
		return mongo;
	} catch (e) {
		logAndThrowError(`Could not close Mongo connection: ${e.message}`);
	}
};

////////////////////////////////////////////////////////////////////////////////
module.exports = {
	connectToMongo,
	closeMongo,
	dbExists,
};
