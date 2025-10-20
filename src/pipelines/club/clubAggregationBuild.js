////////////////////////////////////////////////////////////////////////////////
// When a club is created or updated, we need to build its aggregated view.
// We must also update any referenced SGO, team, or venue aggregation documents
// to maintain bidirectional consistency.
////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug, warn } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline, queryForClubAggregationDoc } = require('./clubAggregationPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
/**
 * Processes a club aggregation by building and updating materialized aggregation data.
 *
 * @async
 * @function processClub
 * @param {Object} config - Configuration object containing MongoDB settings
 * @param {string} config.mongo.matAggCollectionName - Name of the materialized aggregation collection
 * @param {Object} mongo - MongoDB connection object with db property
 * @param {string} clubIdScope - External ID scope for the club
 * @param {string} clubId - External ID of the club to process
 * @param {string} requestId - Unique identifier for tracking the request
 * @param {boolean} [updatedReferences=true] - Whether to update references after aggregation
 * @returns {Promise<Object|number|null>} Returns the new aggregation document, 404 if club not found, or null on failure
 * @throws {Error} Throws error if configuration is invalid or required parameters are missing
 *
 * @description
 * 1. Validates configuration and parameters
 * 2. Checks if the club exists in the database
 * 3. Runs aggregation pipeline to build materialized view
 * 4. Compares old and new aggregation documents
 * 5. Updates references if requested and aggregation succeeded
 */
async function processClub(config, mongo, clubIdScope, clubId, requestId, updatedReferences = true) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!clubId || !clubIdScope) throw new Error('Invalid parameters: clubId and clubIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`clubIdScope=${clubIdScope}, clubId=${clubId}`, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Check if club exists before running expensive pipeline
	const clubExists = await mongo.db.collection('clubs').countDocuments({ _externalId: clubId, _externalIdScope: clubIdScope }, { limit: 1 });
	//////////////////////////////////////////////////////////////////////////////
	if (clubExists === 0) {
		debug(`Club not found: ${clubId}@${clubIdScope}`, requestId);
		return 404;
	}
	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, clubIdScope, clubId);
	const clubAggregationDocQuery = queryForClubAggregationDoc(clubId, clubIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the previous version of the club aggregation (if it exists)
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(clubAggregationDocQuery);
	//////////////////////////////////////////////////////////////////////////////
	// Build the club aggregation view
	await runPipeline(mongo, 'clubs', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the club aggregation and calculate new outbound keys
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(clubAggregationDocQuery);
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
module.exports = { processClub };
