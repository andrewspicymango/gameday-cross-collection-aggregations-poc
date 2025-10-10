///////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline } = require('./teamAggregationPipeline');
const { queryForTeamAggregationDoc } = require('./teamAggregationPipeline');

////////////////////////////////////////////////////////////////////////////////
// Process team updates
async function processTeam(config, mongo, teamIdScope, teamId, requestId) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!teamId || !teamIdScope) throw new Error('Invalid parameters: teamId and teamIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`teamIdScope=${teamIdScope}, teamId=${teamId}`, requestId);
	const pipelineObj = pipeline(config, teamIdScope, teamId);
	const query = queryForTeamAggregationDoc(teamId, teamIdScope);
	await runPipeline(mongo, 'teams', pipelineObj, requestId);
	const doc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(query);
	return doc;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processTeam };
