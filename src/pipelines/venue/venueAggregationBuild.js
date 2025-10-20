////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline } = require('./venuesAggregationPipeline');
const { queryForVenueAggregationDoc } = require('./venuesAggregationPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
/**
 * Processes a venue aggregation by building and updating venue data through MongoDB pipelines.
 *
 * @async
 * @function processVenue
 * @param {Object} config - Configuration object containing MongoDB settings
 * @param {string} config.mongo.matAggCollectionName - Name of the materialized aggregation collection
 * @param {Object} mongo - MongoDB connection object with database access
 * @param {string} venueIdScope - External ID scope for the venue
 * @param {string} venueId - External ID of the venue to process
 * @param {string} requestId - Unique identifier for tracking the request
 * @param {boolean} [updatedReferences=true] - Whether to update references after aggregation
 *
 * @returns {Promise<Object|number|null>} Returns:
 *   - 404 if venue doesn't exist
 *   - null if aggregation fails or references aren't updated
 *   - Object containing the new aggregation document if successful
 *
 * @throws {Error} Throws if configuration is invalid or required parameters are missing
 *
 * @description
 * This function performs the following operations:
 * 1. Validates configuration and parameters
 * 2. Checks if the venue exists in the database
 * 3. Retrieves the previous aggregation document
 * 4. Runs the aggregation pipeline to build new venue data
 * 5. Compares old and new documents to update references if needed
 * 6. Returns the resulting aggregation document
 */
async function processVenue(config, mongo, venueIdScope, venueId, requestId, updatedReferences = true) {
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
module.exports = { processVenue };
