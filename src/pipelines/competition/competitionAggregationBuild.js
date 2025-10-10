///////////////////////////////////////////////////////////////////////////////
// When a competition is created or updated, we need to build its aggregated view
// We do not need to validate other materialized views as competitions are not
// referenced by other entities (e.g. events) in a way that requires updating the
// competition view when those entities change.
////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline } = require('./competitionAggregationPipeline');
const { queryForCompetitionAggregationDoc } = require('./competitionAggregationPipeline');

////////////////////////////////////////////////////////////////////////////////
// Process competition updates
async function processCompetition(config, mongo, competitionIdScope, competitionId, requestId) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!competitionId || !competitionIdScope) throw new Error('Invalid parameters: competitionId and competitionIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`competitionIdScope=${competitionIdScope}, competitionId=${competitionId}`, requestId);
	const pipelineObj = pipeline(config, competitionIdScope, competitionId);
	const query = queryForCompetitionAggregationDoc(competitionId, competitionIdScope);
	await runPipeline(mongo, 'competitions', pipelineObj, requestId);
	const doc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(query);
	return doc;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processCompetition };
