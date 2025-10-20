////////////////////////////////////////////////////////////////////////////////
// Builds or rebuilds nation aggregation documents and updates inbound references
// (teams, venues, sgos) to maintain bidirectional consistency.
////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug, warn } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');
const { RankingKeyClass } = require('./rankingKeyClass.js');

////////////////////////////////////////////////////////////////////////////////
/**
 * Processes a ranking resource by validating inputs, checking existence,
 * running aggregation pipeline, and optionally updating references.
 *
 * @async
 * @function processRanking
 * @param {Object} config - Configuration object containing mongo settings
 * @param {string} config.mongo.matAggCollectionName - Name of materialized aggregation collection
 * @param {Object} mongo - MongoDB connection object with db property
 * @param {RankingKeyClass} rk - Instance of RankingKeyClass for ranking operations
 * @param {string} requestId - Unique identifier for request logging
 * @param {boolean} [updatedReferences=true] - Whether to update reference documents
 * @returns {Promise<Object|number|null>} Returns aggregation document on success,
 *   404 if ranking not found, or null on failure
 * @throws {Error} When config is invalid, rk is not RankingKeyClass instance,
 *   or rk validation fails
 *
 * @description Validates ranking key, checks if ranking exists in database,
 * runs aggregation pipeline to build materialized view, and optionally
 * updates dependent reference documents based on changes.
 */
async function processRanking(config, mongo, rk, requestId, updatedReferences = true) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	//////////////////////////////////////////////////////////////////////////////
	// Check if parameter is an instance of RankingKeyClass
	if (!(rk instanceof RankingKeyClass)) {
		throw new Error('rk must be an instance of RankingKeyClass');
	}
	//////////////////////////////////////////////////////////////////////////////
	// Additional validation
	if (!rk.validate()) {
		throw new Error('Invalid RankingKeyClass: validation failed');
	}
	//////////////////////////////////////////////////////////////////////////////
	debug(`Processing Ranking resource`, requestId);
	const query = rk.rankingDocumentQuery();
	const exists = await mongo.db.collection('rankings').countDocuments(query, { limit: 1 });
	if (exists === 0) {
		debug(`Ranking not found: ${JSON.stringify(query)}`, requestId);
		return 404;
	}
	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = rk.pipeline(config);
	const staffAggQuery = rk.aggregationDocQuery();
	//////////////////////////////////////////////////////////////////////////////
	// Get the original aggregation document (if it exists) and calculate old outbound keys
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(staffAggQuery);
	//////////////////////////////////////////////////////////////////////////////
	// Run the pipeline to build the aggregation document
	await runPipeline(mongo, 'rankings', pipelineObj, requestId);
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(staffAggQuery);
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
module.exports = { processRanking };
