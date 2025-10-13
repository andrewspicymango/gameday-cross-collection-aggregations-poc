////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { keySeparator } = require('../constants');
const { updateResourceReferencesInAggregationDocs } = require('../updateResourceReferencesInAggregationDocs');
const { pipeline, queryForSgoAggregationDoc } = require('./sgoAggregationPipeline');

////////////////////////////////////////////////////////////////////////////////
/**
 * Builds or rebuilds SGO aggregation documents and synchronizes cross-references.
 *
 * This function performs a complete aggregation build for an SGO, capturing all
 * related resources (competitions, stages, events, teams, sports persons, venues) and
 * maintaining referential integrity across the materialized aggregation collection.
 *
 * Process flow:
 * 1. Validates SGO existence to avoid expensive pipeline execution on missing data
 * 2. Captures current aggregation state (old keys) before rebuilding
 * 3. Executes aggregation pipeline to rebuild SGO materialized view
 * 4. Compares old vs new keys to identify relationship changes
 * 5. Updates cross-references in related resource aggregations:
 *    - Competition aggregations (add/remove SGO reference for membership changes)
 *    - Team aggregations (add/remove SGO reference for membership changes)
 *    - Venue aggregations (add/remove SGO reference for membership changes)
 *
 * The function ensures that when an SGO changes its membership associations,
 * all affected aggregation documents are updated to maintain consistency.
 *
 * @async
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {string} sgoIdScope - External scope identifier for the SGO
 * @param {string} sgoId - External identifier for the SGO
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<Object|number>} New aggregation document or 404 if SGO not found
 * @throws {Error} If configuration is invalid or required parameters are missing
 */
async function processSgo(config, mongo, sgoIdScope, sgoId, requestId) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!sgoId || !sgoIdScope) throw new Error('Invalid parameters: sgoId and sgoIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`sgoIdScope=${sgoIdScope}, sgoId=${sgoId}`, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Check if SGO exists before running expensive pipeline
	const sgoExists = await mongo.db.collection('sgos').countDocuments({ _externalId: sgoId, _externalIdScope: sgoIdScope }, { limit: 1 });
	//////////////////////////////////////////////////////////////////////////////
	if (sgoExists === 0) {
		debug(`SGO not found: ${sgoId}@${sgoIdScope}`, requestId);
		return 404;
	}

	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, sgoIdScope, sgoId);
	const sgoAggregationDocQuery = queryForSgoAggregationDoc(sgoId, sgoIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the previous version of the SGO aggregation (if it exists) and calculate old keys
	// An SGO has inbound references from:
	// - competitions (via sgoMemberships)
	// - teams (via sgoMemberships)
	// - clubs (via sgoMemberships)
	// - venues (via sgoMemberships)
	// - nations (via sgoMemberships)
	// - sgos (via sgoMemberships)
	// Note: stages, events, sportsPersons are derived through other relationships
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(sgoAggregationDocQuery);
	const oldCompetitionExternalKeys = oldAggregationDoc?.competitionKeys || [];
	const oldTeamExternalKeys = oldAggregationDoc?.teamKeys || [];
	const oldClubExternalKeys = oldAggregationDoc?.clubKeys || [];
	const oldVenueExternalKeys = oldAggregationDoc?.venueKeys || [];
	const oldNationExternalKeys = oldAggregationDoc?.nationKeys || [];
	const oldSgoExternalKeys = oldAggregationDoc?.sgoKeys || [];
	//////////////////////////////////////////////////////////////////////////////
	// Build the SGO aggregation view
	await runPipeline(mongo, 'sgos', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the SGO aggregation and calculate new outbound keys
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(sgoAggregationDocQuery);
	//////////////////////////////////////////////////////////////////////////////
	// FIX REFERENCES
	const sgoObjectId = newAggregationDoc?.gamedayId;
	const sgoKey = `${sgoId}${keySeparator}${sgoIdScope}`;
	const sgoResourceReference = { resourceType: 'sgo', externalKey: sgoKey, objectId: sgoObjectId };
	//////////////////////////////////////////////////////////////////////////////
	// Update competition references
	await updateCompetitionReferences(newAggregationDoc, oldCompetitionExternalKeys, mongo, config, sgoResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Update team references
	await updateTeamReferences(newAggregationDoc, oldTeamExternalKeys, mongo, config, sgoResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Update venue references
	await updateVenueReferences(newAggregationDoc, oldVenueExternalKeys, mongo, config, sgoResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Update club references
	await updateClubReferences(newAggregationDoc, oldClubExternalKeys, mongo, config, sgoResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Update nation references
	await updateNationReferences(newAggregationDoc, oldNationExternalKeys, mongo, config, sgoResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Update sgo references
	await updateSgoReferences(newAggregationDoc, oldSgoExternalKeys, mongo, config, sgoResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Updates competition aggregation documents when an SGO's competition membership changes.
 *
 * Compares old vs new competition external keys to identify membership changes, then updates
 * the affected competition aggregation documents to add/remove SGO references accordingly.
 * This maintains bidirectional consistency between SGO and competition aggregations.
 *
 * Process:
 * 1. Identifies competitions losing this SGO (in old but not new keys)
 * 2. Removes SGO reference from those competition aggregations
 * 3. Identifies competitions gaining this SGO (in new but not old keys)
 * 4. Adds SGO reference to those competition aggregations
 *
 * @async
 * @param {Object} newAggregationDoc - Updated SGO aggregation document
 * @param {string[]} oldCompetitionExternalKeys - Previous competition keys from SGO aggregation
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} sgoResourceReference - SGO reference object (resourceType, externalKey, objectId)
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<void>}
 */
async function updateCompetitionReferences(newAggregationDoc, oldCompetitionExternalKeys, mongo, config, sgoResourceReference, requestId) {
	const newCompetitionExternalKeys = newAggregationDoc?.competitionKeys || [];

	// Remove SGO from competitions that no longer have this SGO
	const competitionExternalKeysToRemoveSgoFrom = _.difference(oldCompetitionExternalKeys, newCompetitionExternalKeys);
	for (const oldCompetitionExternalKey of competitionExternalKeysToRemoveSgoFrom) {
		const competitionAggregationDocToRemoveSgoFrom = { resourceType: 'competition', externalKey: { old: oldCompetitionExternalKey || null, new: null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, competitionAggregationDocToRemoveSgoFrom, sgoResourceReference, requestId);
	}

	// Add SGO to competitions that now have this SGO
	const competitionExternalKeysToAddSgoTo = _.difference(newCompetitionExternalKeys, oldCompetitionExternalKeys);
	for (const newCompetitionExternalKey of competitionExternalKeysToAddSgoTo) {
		const competitionAggregationDocToAddSgoTo = { resourceType: 'competition', externalKey: { old: null, new: newCompetitionExternalKey || null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, competitionAggregationDocToAddSgoTo, sgoResourceReference, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Updates team aggregation documents when an SGO's team membership changes.
 *
 * Compares old vs new team external keys to identify membership changes, then updates
 * the affected team aggregation documents to add/remove SGO references accordingly.
 * This maintains bidirectional consistency between SGO and team aggregations.
 *
 * Process:
 * 1. Identifies teams losing this SGO (in old but not new keys)
 * 2. Removes SGO reference from those team aggregations
 * 3. Identifies teams gaining this SGO (in new but not old keys)
 * 4. Adds SGO reference to those team aggregations
 *
 * @async
 * @param {Object} newAggregationDoc - Updated SGO aggregation document
 * @param {string[]} oldTeamExternalKeys - Previous team keys from SGO aggregation
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} sgoResourceReference - SGO reference object (resourceType, externalKey, objectId)
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<void>}
 */
async function updateTeamReferences(newAggregationDoc, oldTeamExternalKeys, mongo, config, sgoResourceReference, requestId) {
	const newTeamExternalKeys = newAggregationDoc?.teamKeys || [];

	// Remove SGO from teams that no longer have this SGO
	const teamExternalKeysToRemoveSgoFrom = _.difference(oldTeamExternalKeys, newTeamExternalKeys);
	for (const oldTeamExternalKey of teamExternalKeysToRemoveSgoFrom) {
		const teamAggregationDocToRemoveSgoFrom = { resourceType: 'team', externalKey: { old: oldTeamExternalKey || null, new: null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, teamAggregationDocToRemoveSgoFrom, sgoResourceReference, requestId);
	}

	// Add SGO to teams that now have this SGO
	const teamExternalKeysToAddSgoTo = _.difference(newTeamExternalKeys, oldTeamExternalKeys);
	for (const newTeamExternalKey of teamExternalKeysToAddSgoTo) {
		const teamAggregationDocToAddSgoTo = { resourceType: 'team', externalKey: { old: null, new: newTeamExternalKey || null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, teamAggregationDocToAddSgoTo, sgoResourceReference, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Updates venue aggregation documents when an SGO's venue membership changes.
 *
 * Compares old vs new venue external keys to identify membership changes, then updates
 * the affected venue aggregation documents to add/remove SGO references accordingly.
 * This maintains bidirectional consistency between SGO and venue aggregations.
 *
 * Process:
 * 1. Identifies venues losing this SGO (in old but not new keys)
 * 2. Removes SGO reference from those venue aggregations
 * 3. Identifies venues gaining this SGO (in new but not old keys)
 * 4. Adds SGO reference to those venue aggregations
 *
 * @async
 * @param {Object} newAggregationDoc - Updated SGO aggregation document
 * @param {string[]} oldVenueExternalKeys - Previous venue keys from SGO aggregation
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} sgoResourceReference - SGO reference object (resourceType, externalKey, objectId)
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<void>}
 */
async function updateVenueReferences(newAggregationDoc, oldVenueExternalKeys, mongo, config, sgoResourceReference, requestId) {
	const newVenueExternalKeys = newAggregationDoc?.venueKeys || [];

	// Remove SGO from venues that no longer have this SGO
	const venueExternalKeysToRemoveSgoFrom = _.difference(oldVenueExternalKeys, newVenueExternalKeys);
	for (const oldVenueExternalKey of venueExternalKeysToRemoveSgoFrom) {
		const venueAggregationDocToRemoveSgoFrom = { resourceType: 'venue', externalKey: { old: oldVenueExternalKey || null, new: null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, venueAggregationDocToRemoveSgoFrom, sgoResourceReference, requestId);
	}

	// Add SGO to venues that now have this SGO
	const venueExternalKeysToAddSgoTo = _.difference(newVenueExternalKeys, oldVenueExternalKeys);
	for (const newVenueExternalKey of venueExternalKeysToAddSgoTo) {
		const venueAggregationDocToAddSgoTo = { resourceType: 'venue', externalKey: { old: null, new: newVenueExternalKey || null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, venueAggregationDocToAddSgoTo, sgoResourceReference, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Updates club aggregation documents when an SGO's club membership changes.
 *
 * Compares old vs new club external keys to identify membership changes, then updates
 * the affected club aggregation documents to add/remove SGO references accordingly.
 * This maintains bidirectional consistency between SGO and club aggregations.
 *
 * Process:
 * 1. Identifies clubs losing this SGO (in old but not new keys)
 * 2. Removes SGO reference from those club aggregations
 * 3. Identifies clubs gaining this SGO (in new but not old keys)
 * 4. Adds SGO reference to those club aggregations
 *
 * @async
 * @param {Object} newAggregationDoc - Updated SGO aggregation document
 * @param {string[]} oldClubExternalKeys - Previous club keys from SGO aggregation
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} sgoResourceReference - SGO reference object (resourceType, externalKey, objectId)
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<void>}
 */
async function updateClubReferences(newAggregationDoc, oldClubExternalKeys, mongo, config, sgoResourceReference, requestId) {
	const newClubExternalKeys = newAggregationDoc?.clubKeys || [];

	// Remove SGO from clubs that no longer have this SGO
	const clubExternalKeysToRemoveSgoFrom = _.difference(oldClubExternalKeys, newClubExternalKeys);
	for (const oldClubExternalKey of clubExternalKeysToRemoveSgoFrom) {
		const clubAggregationDocToRemoveSgoFrom = { resourceType: 'club', externalKey: { old: oldClubExternalKey || null, new: null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, clubAggregationDocToRemoveSgoFrom, sgoResourceReference, requestId);
	}

	// Add SGO to clubs that now have this SGO
	const clubExternalKeysToAddSgoTo = _.difference(newClubExternalKeys, oldClubExternalKeys);
	for (const newClubExternalKey of clubExternalKeysToAddSgoTo) {
		const clubAggregationDocToAddSgoTo = { resourceType: 'club', externalKey: { old: null, new: newClubExternalKey || null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, clubAggregationDocToAddSgoTo, sgoResourceReference, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Updates nation aggregation documents when an SGO's nation membership changes.
 *
 * Compares old vs new nation external keys to identify membership changes, then updates
 * the affected nation aggregation documents to add/remove SGO references accordingly.
 * This maintains bidirectional consistency between SGO and nation aggregations.
 *
 * Process:
 * 1. Identifies nations losing this SGO (in old but not new keys)
 * 2. Removes SGO reference from those nation aggregations
 * 3. Identifies nations gaining this SGO (in new but not old keys)
 * 4. Adds SGO reference to those nation aggregations
 *
 * @async
 * @param {Object} newAggregationDoc - Updated SGO aggregation document
 * @param {string[]} oldNationExternalKeys - Previous nation keys from SGO aggregation
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} sgoResourceReference - SGO reference object (resourceType, externalKey, objectId)
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<void>}
 */
async function updateNationReferences(newAggregationDoc, oldNationExternalKeys, mongo, config, sgoResourceReference, requestId) {
	const newNationExternalKeys = newAggregationDoc?.nationKeys || [];

	// Remove SGO from nations that no longer have this SGO
	const nationExternalKeysToRemoveSgoFrom = _.difference(oldNationExternalKeys, newNationExternalKeys);
	for (const oldNationExternalKey of nationExternalKeysToRemoveSgoFrom) {
		const nationAggregationDocToRemoveSgoFrom = { resourceType: 'nation', externalKey: { old: oldNationExternalKey || null, new: null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, nationAggregationDocToRemoveSgoFrom, sgoResourceReference, requestId);
	}

	// Add SGO to nations that now have this SGO
	const nationExternalKeysToAddSgoTo = _.difference(newNationExternalKeys, oldNationExternalKeys);
	for (const newNationExternalKey of nationExternalKeysToAddSgoTo) {
		const nationAggregationDocToAddSgoTo = { resourceType: 'nation', externalKey: { old: null, new: newNationExternalKey || null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, nationAggregationDocToAddSgoTo, sgoResourceReference, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Updates SGO aggregation documents when an SGO's SGO membership changes.
 *
 * Compares old vs new SGO external keys to identify membership changes, then updates
 * the affected SGO aggregation documents to add/remove SGO references accordingly.
 * This maintains bidirectional consistency between SGO and SGO aggregations.
 *
 * Process:
 * 1. Identifies SGOs losing this SGO (in old but not new keys)
 * 2. Removes SGO reference from those SGO aggregations
 * 3. Identifies SGOs gaining this SGO (in new but not old keys)
 * 4. Adds SGO reference to those SGO aggregations
 *
 * @async
 * @param {Object} newAggregationDoc - Updated SGO aggregation document
 * @param {string[]} oldSgoExternalKeys - Previous SGO keys from SGO aggregation
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} sgoResourceReference - SGO reference object (resourceType, externalKey, objectId)
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<void>}
 */
async function updateSgoReferences(newAggregationDoc, oldSgoExternalKeys, mongo, config, sgoResourceReference, requestId) {
	const newSgoExternalKeys = newAggregationDoc?.sgoKeys || [];

	// Remove SGO from SGOs that no longer have this SGO
	const sgoExternalKeysToRemoveSgoFrom = _.difference(oldSgoExternalKeys, newSgoExternalKeys);
	for (const oldSgoExternalKey of sgoExternalKeysToRemoveSgoFrom) {
		const sgoAggregationDocToRemoveSgoFrom = { resourceType: 'sgo', externalKey: { old: oldSgoExternalKey || null, new: null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, sgoAggregationDocToRemoveSgoFrom, sgoResourceReference, requestId);
	}

	// Add SGO to SGOs that now have this SGO
	const sgoExternalKeysToAddSgoTo = _.difference(newSgoExternalKeys, oldSgoExternalKeys);
	for (const newSgoExternalKey of sgoExternalKeysToAddSgoTo) {
		const sgoAggregationDocToAddSgoTo = { resourceType: 'sgo', externalKey: { old: null, new: newSgoExternalKey || null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, sgoAggregationDocToAddSgoTo, sgoResourceReference, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
module.exports = {
	processSgo,
};
