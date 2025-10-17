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
async function processTeam(config, mongo, teamIdScope, teamId, requestId) {
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
	// Update references based on changes
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
module.exports = { processTeam };
