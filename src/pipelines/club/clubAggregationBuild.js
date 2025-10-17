////////////////////////////////////////////////////////////////////////////////
// When a club is created or updated, we need to build its aggregated view.
// We must also update any referenced SGO, team, or venue aggregation documents
// to maintain bidirectional consistency.
////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline, queryForClubAggregationDoc } = require('./clubAggregationPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
// Process club updates
/**
 * Builds or rebuilds club aggregation documents and synchronizes cross-references.
 *
 * This function performs a complete aggregation build for a club, capturing all
 * related resources (SGOs, teams, venues) and maintaining referential integrity
 * across the materialized aggregation collection.
 *
 * Process flow:
 * 1. Validates club existence to avoid expensive pipeline execution on missing data
 * 2. Captures current aggregation state (old keys) before rebuilding
 * 3. Executes aggregation pipeline to rebuild club materialized view
 * 4. Compares old vs new keys to identify relationship changes
 * 5. Updates cross-references in related resource aggregations:
 *    - SGO aggregations (add/remove club reference for SGO membership changes)
 *    - Team aggregations (add/remove club reference for team membership changes)
 *    - Venue aggregations (add/remove club reference for venue membership changes)
 *
 * The function ensures that when a club changes its SGO/team/venue associations,
 * all affected aggregation documents are updated to maintain consistency.
 *
 * @async
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {string} clubIdScope - External scope identifier for the club
 * @param {string} clubId - External identifier for the club
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<Object|number>} New aggregation document or 404 if club not found
 * @throws {Error} If configuration is invalid or required parameters are missing
 */
async function processClub(config, mongo, clubIdScope, clubId, requestId) {
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
	if (_.isObject(newAggregationDoc)) {
		const operations = buildOperationsForReferenceChange(oldAggregationDoc, newAggregationDoc);
		await executeOperationsForReferenceChange(mongo, config, operations, requestId);
	} else {
		warn(`Failed to build new aggregation document`, requestId);
		return null;
	}
	//////////////////////////////////////////////////////////////////////////////
	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processClub };
