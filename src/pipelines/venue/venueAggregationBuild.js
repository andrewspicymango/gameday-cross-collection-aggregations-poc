////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline } = require('./venuesAggregationPipeline');
const { queryForVenueAggregationDoc } = require('./venuesAggregationPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
async function processVenue(config, mongo, venueIdScope, venueId, requestId) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!venueId || !venueIdScope) throw new Error('Invalid parameters: venueId and venueIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`venueIdScope=${venueIdScope}, venueId=${venueId}`, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Check if venue exists before running expensive pipeline
	const venueExists = await mongo.db.collection('venues').countDocuments({ _externalId: venueId, _externalIdScope: venueIdScope }, { limit: 1 });
	//////////////////////////////////////////////////////////////////////////////
	if (venueExists === 0) {
		debug(`Venue not found: ${venueId}@${venueIdScope}`, requestId);
		return 404;
	}
	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, venueIdScope, venueId);
	const venueAggregationDocQuery = queryForVenueAggregationDoc(venueId, venueIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the previous version of the venue aggregation
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(venueAggregationDocQuery);
	//////////////////////////////////////////////////////////////////////////////
	// Build the venue aggregation view
	await runPipeline(mongo, 'venues', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the venue aggregation and calculate new outbound keys
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(venueAggregationDocQuery);
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
module.exports = { processVenue };
