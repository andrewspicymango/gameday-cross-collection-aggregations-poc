///////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline } = require('./teamAggregationPipeline');
const { queryForTeamAggregationDoc } = require('./teamAggregationPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
// Process team updates
/**
 * Processes a team aggregation by building a materialized view and updating references.
 *
 * @async
 * @function processTeam
 * @param {Object} config - Configuration object containing MongoDB settings
 * @param {string} config.mongo.matAggCollectionName - Name of the materialized aggregation collection
 * @param {Object} mongo - MongoDB connection object
 * @param {Object} mongo.db - MongoDB database instance
 * @param {string} teamIdScope - External ID scope for the team
 * @param {string} teamId - External ID of the team to process
 * @param {string} requestId - Unique identifier for tracking the request
 * @param {boolean} [updatedReferences=true] - Whether to update references after aggregation
 * @returns {Promise<Object|number|null>} Returns the new aggregation document on success,
 *   404 if team not found, or null on failure
 * @throws {Error} When configuration is invalid or required parameters are missing
 *
 * @description
 * This function performs the following operations:
 * 1. Validates configuration and parameters
 * 2. Checks if the team exists in the database
 * 3. Retrieves the previous aggregation document
 * 4. Runs the aggregation pipeline to build a new materialized view
 * 5. Compares old and new documents to determine reference updates needed
 * 6. Executes reference update operations if enabled
 *
 * @example
 * const result = await processTeam(config, mongo, 'external', 'team123', 'req456');
 */
async function processTeam(config, mongo, teamIdScope, teamId, requestId, updatedReferences = true) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!teamId || !teamIdScope) throw new Error('Invalid parameters: teamId and teamIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`teamIdScope=${teamIdScope}, teamId=${teamId}`, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Check if team exists before running expensive pipeline
	const teamExists = await mongo.db.collection('teams').countDocuments({ _externalId: teamId, _externalIdScope: teamIdScope }, { limit: 1 });
	//////////////////////////////////////////////////////////////////////////////
	if (teamExists === 0) {
		debug(`Team not found: ${teamId}@${teamIdScope}`, requestId);
		return 404;
	}
	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, teamIdScope, teamId);
	const teamAggregationDocQuery = queryForTeamAggregationDoc(teamId, teamIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the previous version of the team aggregation
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(teamAggregationDocQuery);
	//////////////////////////////////////////////////////////////////////////////
	// Build the team aggregation view
	await runPipeline(mongo, 'teams', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the teams aggregation and calculate new outbound keys
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(teamAggregationDocQuery);
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
module.exports = { processTeam };
