require('dotenv').config({ quiet: true });
const _ = require(`lodash`);
const express = require('express');
const cors = require('cors');
const uuid = require(`uuid`);
const { closeMongo, connectToMongo, collectionExists, indexExistsOnCollection } = require('./utils/mongoUtils');
const { normalizePort } = require('./utils/generalUtils');
const { setLogLevel, warn, error, info, debug, logAndThrowError } = require('./log.js');
const { send200, send400, send401, send404, send500, sendError } = require('./utils/httpResponseUtils');

////////////////////////////////////////////////////////////////////////////////
// Routes
const healthcheckRoutes = require('./routes/healthcheckRouter.js');
const logRoutes = require('./routes/logRouter.js');
const gamedayDataRouter = require('./routes/gamedayDataRouter.js');

////////////////////////////////////////////////////////////////////////////////
// Constants
const config = require('./config.js');

////////////////////////////////////////////////////////////////////////////////
/**
 * Gracefully shuts down the application by closing the MongoDB connection and exiting the process.
 *
 * This asynchronous handler awaits the closeMongo(mongo) call to attempt a clean shutdown of the
 * database connection, then calls process.exit(0) to terminate the Node process with a success code.
 *
 * Note: process.exit(0) will forcefully terminate the process; any remaining asynchronous work
 * may be interrupted after this call.
 *
 * @async
 * @function shutdownHandler
 * @returns {Promise<void>} Resolves after attempting to close the database connection.
 *                           In practice the process will exit immediately after the close attempt.
 * @throws {Error} If closeMongo rejects, the rejection will propagate unless handled; process.exit may still occur.
 * @see closeMongo
 */
async function shutdownHandler() {
	await closeMongo(mongo);
	process.exit(0);
}

////////////////////////////////////////////////////////////////////////////////
// 404
function _404(req, res, next) {
	const id = uuid.v4();
	debug(`${req.method} ${req.url}${req.hostname != undefined ? ' [called from ' + req.hostname + ']' : ''}`, id);
	debug(`404: No resource at path ${req.path}`, id);
	send404(res);
	return;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Initialize and start the HTTP server for the service.
 *
 * This async entrypoint configures logging, normalizes the configured port,
 * establishes a MongoDB connection, constructs an Express app with JSON and
 * URL-encoded body parsing, mounts application routes and 404 handling,
 * configures CORS for known origins, and starts listening for incoming
 * health-check requests.
 *
 * Side effects:
 * - Reads process.argv for a "-v" flag to set debug logging.
 * - Mutates global/outer-scope `config` (normalizes `config.express.port`).
 * - Calls `connectToMongo` to populate a `mongo` connection.
 * - Creates and starts an Express HTTP server that listens on
 *   `config.server.port`.
 * - Logs informational and error messages via `info`/`error`.
 * - On startup failure, logs the error and exits the process with code -1.
 *
 * @async
 * @function main
 * @returns {Promise<void>} Resolves once the server has been started (or the
 *   function has exited via process termination on error). Errors during
 *   startup are caught internally and result in process.exit(-1).
 */
async function main() {
	try {
		if (process.argv.indexOf(`-v`) != -1) {
			setLogLevel('debug');
		} else {
			setLogLevel('info');
		}

		config.cwd = process.cwd();
		config.express.port = normalizePort(config?.express?.port);
		info(`Trying to start on port ${config.express.port}...`);
		mongo = await connectToMongo(config.mongo);

		////////////////////////////////////////////////////////////////////////////
		// Remember to create collection and indexes as needed
		if (!(await collectionExists(mongo, config?.mongo?.matAggCollectionName))) {
			warn(`Materialised Aggregation Collection ${config.mongo.matAggCollectionName} does not exist - creating...`);
			await mongo.db.createCollection(config.mongo.matAggCollectionName);
			info(`Created collection ${config.mongo.matAggCollectionName}`);
		} else {
			info(`Materialised Aggregation Collection ${config.mongo.matAggCollectionName} exists`);
		}
		if (!(await indexExistsOnCollection(mongo, config.mongo.matAggCollectionName, config.mongo.matAggIndexIdAndScopeName))) {
			warn(`Index ${config.mongo.matAggIndexIdAndScopeName} does not exist on collection ${config.mongo.matAggCollectionName} - creating...`);
			await mongo.db.collection(config.mongo.matAggCollectionName).createIndex(config.mongo.matAggIndexIdAndScope, { unique: true });
			info(`Created index ${config.mongo.matAggIndexIdAndScopeName} on collection ${config.mongo.matAggCollectionName}`);
		} else {
			info(`Index ${config.mongo.matAggIndexIdAndScopeName} exists on collection ${config.mongo.matAggCollectionName}`);
		}

		// Create Express app
		const app = express();
		app.use(express.json());
		app.use(express.urlencoded({ extended: false }));

		////////////////////////////////////////////////////////////////////////////
		// Handle CORS
		const corsOptions = {
			origin: ['http://localhost:3000', 'https://mangoplay.mangodev.co.uk'],
			methods: ['GET', 'POST'],
			credentials: false,
		};

		////////////////////////////////////////////////////////////////////////////
		app.use(cors(corsOptions));
		app.options('/', cors(corsOptions));

		////////////////////////////////////////////////////////////////////////////
		// Routes
		app.use('/healthcheck', healthcheckRoutes);
		app.use('/log', logRoutes);
		app.use('/1-0', gamedayDataRouter);

		////////////////////////////////////////////////////////////////////////////
		// Others
		app.all(/.*/, _404);

		////////////////////////////////////////////////////////////////////////////
		// Start the server
		app.listen(config.express.port, () => {
			info(`Listening for health checks on port ${config.express.port}`);
		});
	} catch (err) {
		error(`Could not start ${config?.serviceName}: ${err.message}`);
		process.exit(-1);
	}
}

////////////////////////////////////////////////////////////////////////////////
process.on('SIGINT', shutdownHandler);
process.on('SIGTERM', shutdownHandler);
main();
