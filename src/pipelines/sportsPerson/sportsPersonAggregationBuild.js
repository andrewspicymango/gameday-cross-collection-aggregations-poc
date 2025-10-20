///////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline } = require('./sportsPersonAggregationPipeline');
const { queryForSportsPersonAggregationDoc } = require('./sportsPersonAggregationPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
/**
 * Processes a sports person by building their aggregation document and updating references.
 *
 * @async
 * @function processSportsPerson
 * @param {Object} config - Configuration object containing mongo settings
 * @param {string} config.mongo.matAggCollectionName - Name of the materialized aggregation collection
 * @param {Object} mongo - MongoDB connection object with db property
 * @param {string} sportsPersonIdScope - External ID scope for the sports person
 * @param {string} sportsPersonId - External ID of the sports person
 * @param {string} requestId - Unique identifier for the request (used for logging)
 * @param {boolean} [updatedReferences=true] - Whether to update references after aggregation
 * @returns {Promise<Object|number|null>} Returns the new aggregation document on success,
 *   404 if sports person not found, or null if operation failed
 * @throws {Error} Throws error for invalid configuration or missing required parameters
 *
 * @description
 * This function validates the sports person exists, runs an aggregation pipeline to build
 * a materialized view, compares old and new aggregation documents, and optionally updates
 * references based on changes detected.
 */
async function processSportsPerson(config, mongo, sportsPersonIdScope, sportsPersonId, requestId, updatedReferences = true) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!sportsPersonId || !sportsPersonIdScope) throw new Error('Invalid parameters: sportsPersonId and sportsPersonIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`sportsPersonIdScope=${sportsPersonIdScope}, sportsPersonId=${sportsPersonId}`, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Check if sports person exists before running expensive pipeline
	const sportsPersonExists = await mongo.db.collection('sportsPersons').countDocuments({ _externalId: sportsPersonId, _externalIdScope: sportsPersonIdScope }, { limit: 1 });
	//////////////////////////////////////////////////////////////////////////////
	if (sportsPersonExists === 0) {
		debug(`Sports person not found: ${sportsPersonId}@${sportsPersonIdScope}`, requestId);
		return 404;
	}
	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, sportsPersonIdScope, sportsPersonId);
	const sportsPersonAggregationDocQuery = queryForSportsPersonAggregationDoc(sportsPersonId, sportsPersonIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the previous version of the sports person aggregation
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(sportsPersonAggregationDocQuery);
	//////////////////////////////////////////////////////////////////////////////
	// Build the sports person aggregation view
	await runPipeline(mongo, 'sportsPersons', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the sports person aggregation and calculate new outbound keys
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(sportsPersonAggregationDocQuery);
	//////////////////////////////////////////////////////////////////////////////
	if (!_.isObject(newAggregationDoc)) {
		warn(`Failed to build new aggregation document`, requestId);
		return null;
	}
	//////////////////////////////////////////////////////////////////////////////
	// Compare old and new aggregation documents to determine if references need to be updated
	if (updatedReferences === true) {
		const operations = buildOperationsForReferenceChange(oldAggregationDoc, newAggregationDoc);
		await executeOperationsForReferenceChange(mongo, config, operations, requestId);
	}
	//////////////////////////////////////////////////////////////////////////////
	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processSportsPerson };
