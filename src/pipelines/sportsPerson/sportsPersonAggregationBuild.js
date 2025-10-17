///////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline } = require('./sportsPersonAggregationPipeline');
const { queryForSportsPersonAggregationDoc } = require('./sportsPersonAggregationPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
// Process sports person updates
async function processSportsPerson(config, mongo, sportsPersonIdScope, sportsPersonId, requestId) {
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
module.exports = { processSportsPerson };
