const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline } = require('./eventAggregationPipeline');
const { queryForEventAggregationDoc } = require('./eventAggregationPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
/**
 * Builds or rebuilds event aggregation documents and synchronizes cross-references.
 *
 * This function performs a complete aggregation build for an event, capturing all
 * related resources (stages, venues, teams, sports persons) and maintaining referential
 * integrity across the materialized aggregation collection.
 *
 * Process flow:
 * 1. Validates event existence to avoid expensive pipeline execution on missing data
 * 2. Captures current aggregation state (old keys) before rebuilding
 * 3. Executes aggregation pipeline to rebuild event materialized view
 * 4. Compares old vs new keys to identify relationship changes
 * 5. Updates cross-references in related resource aggregations:
 *    - Stage aggregations (add/remove event reference)
 *    - Venue aggregations (add/remove event reference)
 *    - Team aggregations (add/remove event reference for participant changes)
 *    - Sports person aggregations (add/remove event reference for participant changes)
 *
 * The function ensures that when an event moves between stages/venues or changes
 * participants, all affected aggregation documents are updated to maintain consistency.
 *
 * @async
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {string} eventIdScope - External scope identifier for the event
 * @param {string} eventId - External identifier for the event
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<Object|number>} New aggregation document or 404 if event not found
 * @throws {Error} If configuration is invalid or required parameters are missing
 */
async function processEvent(config, mongo, eventIdScope, eventId, requestId) {
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
	const operations = buildOperationsForReferenceChange(oldAggregationDoc, newAggregationDoc);
	await executeOperationsForReferenceChange(mongo, config, operations, requestId);
	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processEvent };
