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
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
// Process competition updates
async function processCompetition(config, mongo, competitionIdScope, competitionId, requestId) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!competitionId || !competitionIdScope) throw new Error('Invalid parameters: competitionId and competitionIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`competitionIdScope=${competitionIdScope}, competitionId=${competitionId}`, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Check if competition exists before running expensive pipeline
	const competitionExists = await mongo.db.collection('competitions').countDocuments({ _externalId: competitionId, _externalIdScope: competitionIdScope }, { limit: 1 });
	//////////////////////////////////////////////////////////////////////////////
	if (competitionExists === 0) {
		debug(`Competition not found: ${competitionId}@${competitionIdScope}`, requestId);
		return 404;
	}
	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, competitionIdScope, competitionId);
	const competitionAggregationDocQuery = queryForCompetitionAggregationDoc(competitionId, competitionIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the previous version of the competition aggregation (if it exists) and calculate old sgo keys
	// A competition has outbound references to:
	// - SGOs (via sgoMemberships)
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(competitionAggregationDocQuery);
	//////////////////////////////////////////////////////////////////////////////
	// Build the competition aggregation view
	await runPipeline(mongo, 'competitions', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the competition aggregation and calculate new competition keys
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(competitionAggregationDocQuery);
	//////////////////////////////////////////////////////////////////////////////
	const operations = buildOperationsForReferenceChange(oldAggregationDoc, newAggregationDoc);
	await executeOperationsForReferenceChange(mongo, config, operations, requestId);
	//////////////////////////////////////////////////////////////////////////////
	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processCompetition };
