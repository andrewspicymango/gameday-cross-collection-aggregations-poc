const _ = require('lodash');
const { debug, warn } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline } = require('./eventAggregationPipeline');
const { queryForEventAggregationDoc } = require('./eventAggregationPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
/**
 * Processes an event by building and updating its aggregation document.
 *
 * Validates the event exists, runs an aggregation pipeline to build/update the event's
 * materialized view, and optionally updates references if the aggregation changed.
 *
 * @param {Object} config - Configuration object containing mongo settings
 * @param {string} config.mongo.matAggCollectionName - Name of the materialized aggregation collection
 * @param {Object} mongo - MongoDB connection object with db property
 * @param {string} eventIdScope - External ID scope for the event
 * @param {string} eventId - External ID of the event to process
 * @param {string} requestId - Request identifier for logging/debugging
 * @param {boolean} [updatedReferences=true] - Whether to update references after aggregation
 *
 * @returns {Promise<Object|number|null>} Returns the new aggregation document on success,
 *   404 if event not found, or null if aggregation build failed
 *
 * @throws {Error} When config.mongo.matAggCollectionName is invalid or eventId/eventIdScope missing
 */
async function processEvent(config, mongo, eventIdScope, eventId, requestId, updatedReferences = true) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!eventId || !eventIdScope) throw new Error('Invalid parameters: eventId and eventIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`eventIdScope=${eventIdScope}, eventId=${eventId}`, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Check if event exists before running expensive pipeline
	const eventExists = await mongo.db.collection('events').countDocuments({ _externalId: eventId, _externalIdScope: eventIdScope }, { limit: 1 });
	//////////////////////////////////////////////////////////////////////////////
	if (eventExists === 0) {
		debug(`Event not found: ${eventId}@${eventIdScope}`, requestId);
		return 404;
	}
	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, eventIdScope, eventId);
	const eventAggregationDocQuery = queryForEventAggregationDoc(eventId, eventIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the previous version of the event aggregation (if it exists)
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(eventAggregationDocQuery);
	//////////////////////////////////////////////////////////////////////////////
	// Build the event aggregation view
	await runPipeline(mongo, 'events', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the event aggregation
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(eventAggregationDocQuery);
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
module.exports = { processEvent };
