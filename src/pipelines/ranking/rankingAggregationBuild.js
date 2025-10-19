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
async function processRanking(config, mongo, rk, requestId) {
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
module.exports = { processRanking };
