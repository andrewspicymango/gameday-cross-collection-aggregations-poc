const _ = require('lodash');
const { debug, warn } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline } = require('./keyMomentAggregationPipeline');
const { queryForKeyMomentAggregationDoc } = require('./keyMomentAggregationPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
/**
 * Processes a key moment aggregation by validating parameters, checking if the key moment exists,
 * running an aggregation pipeline, and optionally updating references based on document changes.
 *
 * @async
 * @function processKeyMoment
 * @param {Object} config - Configuration object containing mongo settings
 * @param {string} config.mongo.matAggCollectionName - Name of the materialized aggregation collection
 * @param {Object} mongo - MongoDB connection object with db property
 * @param {string} eventIdScope - External event ID scope identifier
 * @param {string} eventId - External event ID
 * @param {string} type - Key moment type
 * @param {string} subType - Key moment sub-type
 * @param {Date|string} dateTime - Date/time of the key moment (converted to Date if string)
 * @param {string} requestId - Request identifier for logging/debugging
 * @param {boolean} [updatedReferences=true] - Whether to update references after aggregation
 * @returns {Promise<Object|null|number>} Returns the new aggregation document, null on failure, or 404 if key moment not found
 * @throws {Error} Throws error for invalid configuration or missing required parameters
 */
async function processKeyMoment(config, mongo, eventIdScope, eventId, type, subType, dateTime, requestId, updatedReferences = true) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!eventId || !eventIdScope) throw new Error('Invalid parameters: eventId and eventIdScope are required');
	if (!_.isDate(new Date(dateTime)) && !_.isDate(dateTime)) throw new Error('Invalid parameters: dateTime must be a valid date string');
	//////////////////////////////////////////////////////////////////////////////
	if (_.isString(dateTime)) dateTime = new Date(dateTime);
	//////////////////////////////////////////////////////////////////////////////
	debug(`keyMomentEventIdScope=${eventIdScope}, keyMomentEventId=${eventId}, keyMomentType=${type}, keyMomentSubType=${subType}, keyMomentDateTime=${dateTime}`, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Check if keyMoment exists before running expensive pipeline
	const queryToCount = { _externalEventIdScope: eventIdScope, _externalEventId: eventId, subType, dateTime };
	const keyMomentExists = await mongo.db.collection('keyMoments').countDocuments(queryToCount, { limit: 1 });
	//////////////////////////////////////////////////////////////////////////////
	if (keyMomentExists === 0) {
		debug(`keyMoment not found: ${eventId}@${eventIdScope}@${type}@${subType}@${dateTime}`, requestId);
		return 404;
	}
	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, eventIdScope, eventId, type, subType, dateTime);
	const keyMomentAggregationDocQuery = queryForKeyMomentAggregationDoc(eventId, eventIdScope, type, subType, dateTime);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the previous version of the event aggregation (if it exists)
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(keyMomentAggregationDocQuery);
	//////////////////////////////////////////////////////////////////////////////
	// Build the event aggregation view
	await runPipeline(mongo, 'keyMoments', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the event aggregation
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(keyMomentAggregationDocQuery);
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
module.exports = { processKeyMoment };
