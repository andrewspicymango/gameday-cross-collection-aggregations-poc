////////////////////////////////////////////////////////////////////////////////
// Builds or rebuilds nation aggregation documents and updates inbound references
// (teams, venues, sgos) to maintain bidirectional consistency.
////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline, queryForNationAggregationDoc } = require('./nationAggregationPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
/**
 * Processes a nation aggregation by rebuilding the materialized aggregation data
 * and updating any references that may have changed.
 *
 * @async
 * @function processNation
 * @param {Object} config - Configuration object containing mongo settings
 * @param {string} config.mongo.matAggCollectionName - Name of the materialized aggregation collection
 * @param {Object} mongo - MongoDB connection object with db property
 * @param {string} nationIdScope - The scope/namespace for the nation ID
 * @param {string} nationId - The external ID of the nation to process
 * @param {string} requestId - Unique identifier for the request (used for debugging)
 * @returns {Promise<Object|number>} Returns 404 if nation not found, otherwise returns the new aggregation document
 * @throws {Error} Throws error if config.mongo.matAggCollectionName is not a string
 * @throws {Error} Throws error if nationId or nationIdScope are missing
 *
 * @description
 * 1. Validates configuration and parameters
 * 2. Checks if the nation exists in the database
 * 3. Captures the current aggregation state for comparison
 * 4. Runs the aggregation pipeline to rebuild data
 * 5. Compares old vs new aggregation and executes reference updates
 */
async function processNation(config, mongo, nationIdScope, nationId, requestId) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!nationId || !nationIdScope) throw new Error('Invalid parameters: nationId and nationIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`nationIdScope=${nationIdScope}, nationId=${nationId}`, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Validate nation exists
	const exists = await mongo.db.collection('nations').countDocuments({ _externalId: nationId, _externalIdScope: nationIdScope }, { limit: 1 });
	if (exists === 0) {
		debug(`Nation not found: ${nationId}@${nationIdScope}`, requestId);
		return 404;
	}
	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, nationIdScope, nationId);
	const nationAggQuery = queryForNationAggregationDoc(nationId, nationIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Previous aggregation (for diff)
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(nationAggQuery);
	//////////////////////////////////////////////////////////////////////////////
	// Rebuild
	await runPipeline(mongo, 'nations', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the nation aggregation and calculate new reference keys
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(nationAggQuery);
	//////////////////////////////////////////////////////////////////////////////
	const operations = buildOperationsForReferenceChange(oldAggregationDoc, newAggregationDoc);
	await executeOperationsForReferenceChange(mongo, config, operations, requestId);
	//////////////////////////////////////////////////////////////////////////////
	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processNation };
