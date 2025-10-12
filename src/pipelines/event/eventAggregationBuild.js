const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { keySeparator } = require('../constants');
const { updateResourceReferencesInAggregationDocs } = require('../updateResourceReferencesInAggregationDocs');
const { pipeline } = require('./eventAggregationPipeline');
const { queryForEventAggregationDoc } = require('./eventAggregationPipeline');

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
	// Retrieve the previous version of the event aggregation (if it exists) and calculate old competition and stage keys
	// A event has outbound references to:
	// - stage
	// - event
	// - teams (via participants)
	// - sportsPersons (via participants)
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(eventAggregationDocQuery);
	const oldStageExternalKey = oldAggregationDoc?.stageKeys[0];
	const oldVenueExternalKey = oldAggregationDoc?.venueKeys[0];
	const oldTeamKeys = oldAggregationDoc?.teamKeys || [];
	const oldSportsPersonKeys = oldAggregationDoc?.sportsPersonKeys || [];
	//////////////////////////////////////////////////////////////////////////////
	// Build the event aggregation view
	await runPipeline(mongo, 'events', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the event aggregation and calculate new competition and stage keys
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(eventAggregationDocQuery);
	const newTeamKeys = newAggregationDoc?.teamKeys || [];
	const newSportsPersonKeys = newAggregationDoc?.sportsPersonKeys || [];
	//////////////////////////////////////////////////////////////////////////////
	// FIX OUTBOUND REFERENCES
	const eventObjectId = newAggregationDoc?.gamedayId;
	const eventKey = `${eventId}${keySeparator}${eventIdScope}`;
	const eventResourceReference = { resourceType: 'event', externalKey: eventKey, objectId: eventObjectId };
	//////////////////////////////////////////////////////////////////////////////
	// _externalStageId
	await updateStageReferences(newAggregationDoc, oldStageExternalKey, mongo, config, eventResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// _externalVenueId
	await updateVenueReferences(newAggregationDoc, oldVenueExternalKey, mongo, config, eventResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// participants._externalTeamId
	await updateTeamReferences(oldTeamKeys, newTeamKeys, mongo, config, eventResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// participants._externalSportsPersonId
	await updateSportsPersonReferences(oldSportsPersonKeys, newSportsPersonKeys, mongo, config, eventResourceReference, requestId);

	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Updates sports person references in aggregation documents when event data changes.
 *
 * Compares old and new sports person keys to determine which aggregation documents
 * need to be updated. Removes event references from sports persons no longer
 * associated with the event, and adds event references to newly associated
 * sports persons.
 *
 * @async
 * @function updateSportsPersonReferences
 * @param {string[]} oldSportsPersonKeys - Array of previous sports person external keys
 * @param {string[]} newSportsPersonKeys - Array of current sports person external keys
 * @param {Object} mongo - MongoDB connection object
 * @param {Object} config - Configuration object containing database settings
 * @param {Object} eventResourceReference - Reference object for the event being processed
 * @param {string} requestId - Unique identifier for tracking the request
 * @returns {Promise<void>} Promise that resolves when all updates are complete
 *
 * @description
 * This function performs a diff operation between old and new sports person keys:
 * - Keys present in old but not in new: removes event reference from those sports persons
 * - Keys present in new but not in old: adds event reference to those sports persons
 * - Keys present in both: no action needed
 */
async function updateSportsPersonReferences(oldSportsPersonKeys, newSportsPersonKeys, mongo, config, eventResourceReference, requestId) {
	const sportsPersonExternalKeysToRemoveEventFrom = oldSportsPersonKeys.filter((oldKey) => !newSportsPersonKeys.includes(oldKey));
	for (const oldSportsPersonKey of sportsPersonExternalKeysToRemoveEventFrom) {
		const sportsPersonAggregationDocToRemoveEventFrom = { resourceType: 'sportsPerson', externalKey: { old: oldSportsPersonKey || null, new: null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, sportsPersonAggregationDocToRemoveEventFrom, eventResourceReference, requestId);
	}
	// Create a set of keys that are in newSportsPersonKeys but not in oldSportsPersonKeys
	// This competition then needs to be added to all sportsPerson aggregation docs for this set
	const sportsPersonExternalKeysToAddEventTo = newSportsPersonKeys.filter((newKey) => !oldSportsPersonKeys.includes(newKey));
	for (const newSportsPersonKey of sportsPersonExternalKeysToAddEventTo) {
		const sportsPersonAggregationDocToAddEventTo = { resourceType: 'sportsPerson', externalKey: { old: null, new: newSportsPersonKey || null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, sportsPersonAggregationDocToAddEventTo, eventResourceReference, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Updates team references in aggregation documents when event team associations change.
 *
 * Compares old and new team keys to determine which teams need to have the event
 * added or removed from their aggregation documents. Handles the synchronization
 * of team-event relationships by:
 * - Removing event references from teams that are no longer associated
 * - Adding event references to newly associated teams
 *
 * @async
 * @function updateTeamReferences
 * @param {string[]} oldTeamKeys - Array of previous team external keys
 * @param {string[]} newTeamKeys - Array of current team external keys
 * @param {Object} mongo - MongoDB connection object
 * @param {Object} config - Configuration object for database operations
 * @param {Object} eventResourceReference - Reference object for the event being updated
 * @param {string} requestId - Unique identifier for tracking the request
 * @returns {Promise<void>} Promise that resolves when all team references are updated
 */
async function updateTeamReferences(oldTeamKeys, newTeamKeys, mongo, config, eventResourceReference, requestId) {
	const teamExternalKeysToRemoveEventFrom = oldTeamKeys.filter((oldKey) => !newTeamKeys.includes(oldKey));
	for (const oldTeamKey of teamExternalKeysToRemoveEventFrom) {
		const teamAggregationDocToRemoveEventFrom = { resourceType: 'team', externalKey: { old: oldTeamKey || null, new: null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, teamAggregationDocToRemoveEventFrom, eventResourceReference, requestId);
	}
	// Create a set of keys that are in newTeamKeys but not in oldTeamKeys
	// This competition then needs to be added to all Team aggregation docs for this set
	const teamExternalKeysToAddEventTo = newTeamKeys.filter((newKey) => !oldTeamKeys.includes(newKey));
	for (const newTeamKey of teamExternalKeysToAddEventTo) {
		const teamAggregationDocToAddEventTo = { resourceType: 'team', externalKey: { old: null, new: newTeamKey || null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, teamAggregationDocToAddEventTo, eventResourceReference, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Updates venue references in aggregation documents when an event's venue association changes.
 *
 * Extracts the new venue external key from the aggregation document and compares it with
 * the previous venue key to update cross-references between event and venue aggregations.
 * This ensures data consistency when events are moved between venues or venue associations
 * are modified.
 *
 * @param {Object} newAggregationDoc - The updated event aggregation document containing venue keys
 * @param {string|null} oldVenueExternalKey - The previous venue external key before the update
 * @param {Object} mongo - MongoDB connection instance for database operations
 * @param {Object} config - Configuration object containing database and collection settings
 * @param {Object} eventResourceReference - Reference object for the event resource being updated
 * @param {string} requestId - Unique identifier for tracking this request across operations
 * @returns {Promise<void>} Promise that resolves when venue references are successfully updated
 */
async function updateVenueReferences(newAggregationDoc, oldVenueExternalKey, mongo, config, eventResourceReference, requestId) {
	const newVenueExternalKey = newAggregationDoc?.venueKeys[0];
	const venueAggregationDocs = { resourceType: 'venue', externalKey: { old: oldVenueExternalKey || null, new: newVenueExternalKey || null } };
	await updateResourceReferencesInAggregationDocs(mongo, config, venueAggregationDocs, eventResourceReference, requestId);
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Updates stage references in aggregation documents when stage keys change.
 *
 * This function handles the propagation of stage key updates across related
 * aggregation documents by creating a stage aggregation document with old and
 * new external keys, then delegating to the resource reference update system.
 *
 * @param {Object} newAggregationDoc - The updated aggregation document containing new stage keys
 * @param {string} oldStageExternalKey - The previous stage external key before update
 * @param {Object} mongo - MongoDB connection/client instance
 * @param {Object} config - Configuration object for the update operation
 * @param {Object} eventResourceReference - Reference object for the event resource
 * @param {string} requestId - Unique identifier for tracking this request
 * @returns {Promise<void>} Promise that resolves when stage references are updated
 */
async function updateStageReferences(newAggregationDoc, oldStageExternalKey, mongo, config, eventResourceReference, requestId) {
	const newStageExternalKey = newAggregationDoc?.stageKeys[0];
	const stageAggregationDoc = { resourceType: 'stage', externalKey: { old: oldStageExternalKey || null, new: newStageExternalKey || null } };
	await updateResourceReferencesInAggregationDocs(mongo, config, stageAggregationDoc, eventResourceReference, requestId);
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processEvent };
