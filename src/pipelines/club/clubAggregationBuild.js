////////////////////////////////////////////////////////////////////////////////
// When a club is created or updated, we need to build its aggregated view.
// We must also update any referenced SGO, team, or venue aggregation documents
// to maintain bidirectional consistency.
////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { keySeparator } = require('../constants');
const { updateResourceReferencesInAggregationDocs } = require('../updateResourceReferencesInAggregationDocs');
const { pipeline, queryForClubAggregationDoc } = require('./clubAggregationPipeline');

////////////////////////////////////////////////////////////////////////////////
// Process club updates
/**
 * Builds or rebuilds club aggregation documents and synchronizes cross-references.
 *
 * This function performs a complete aggregation build for a club, capturing all
 * related resources (SGOs, teams, venues) and maintaining referential integrity
 * across the materialized aggregation collection.
 *
 * Process flow:
 * 1. Validates club existence to avoid expensive pipeline execution on missing data
 * 2. Captures current aggregation state (old keys) before rebuilding
 * 3. Executes aggregation pipeline to rebuild club materialized view
 * 4. Compares old vs new keys to identify relationship changes
 * 5. Updates cross-references in related resource aggregations:
 *    - SGO aggregations (add/remove club reference for SGO membership changes)
 *    - Team aggregations (add/remove club reference for team membership changes)
 *    - Venue aggregations (add/remove club reference for venue membership changes)
 *
 * The function ensures that when a club changes its SGO/team/venue associations,
 * all affected aggregation documents are updated to maintain consistency.
 *
 * @async
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {string} clubIdScope - External scope identifier for the club
 * @param {string} clubId - External identifier for the club
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<Object|number>} New aggregation document or 404 if club not found
 * @throws {Error} If configuration is invalid or required parameters are missing
 */
async function processClub(config, mongo, clubIdScope, clubId, requestId) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!clubId || !clubIdScope) throw new Error('Invalid parameters: clubId and clubIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`clubIdScope=${clubIdScope}, clubId=${clubId}`, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Check if club exists before running expensive pipeline
	const clubExists = await mongo.db.collection('clubs').countDocuments({ _externalId: clubId, _externalIdScope: clubIdScope }, { limit: 1 });
	//////////////////////////////////////////////////////////////////////////////
	if (clubExists === 0) {
		debug(`Club not found: ${clubId}@${clubIdScope}`, requestId);
		return 404;
	}
	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, clubIdScope, clubId);
	const clubAggregationDocQuery = queryForClubAggregationDoc(clubId, clubIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the previous version of the club aggregation (if it exists) and calculate old keys
	// A club has outbound references to:
	// - SGOs (via sgoMemberships)
	// - Teams
	// - Venues
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(clubAggregationDocQuery);
	const oldSgoExternalKeys = oldAggregationDoc?.sgoKeys || [];
	const oldTeamExternalKeys = oldAggregationDoc?.teamKeys || [];
	const oldVenueExternalKeys = oldAggregationDoc?.venueKeys || [];
	//////////////////////////////////////////////////////////////////////////////
	// Build the club aggregation view
	await runPipeline(mongo, 'clubs', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the club aggregation and calculate new outbound keys
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(clubAggregationDocQuery);
	const newSgoExternalKeys = newAggregationDoc?.sgoKeys || [];
	const newTeamExternalKeys = newAggregationDoc?.teamKeys || [];
	const newVenueExternalKeys = newAggregationDoc?.venueKeys || [];
	//////////////////////////////////////////////////////////////////////////////
	// FIX REFERENCES
	const clubObjectId = newAggregationDoc?.gamedayId;
	const clubKey = `${clubId}${keySeparator}${clubIdScope}`;
	const clubResourceReference = { resourceType: 'club', externalKey: clubKey, objectId: clubObjectId };
	//////////////////////////////////////////////////////////////////////////////
	// sgoMemberships._externalSgoId
	await updateSgoClubReferences(oldSgoExternalKeys, newSgoExternalKeys, mongo, config, clubResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// teams
	await updateTeamClubReferences(oldTeamExternalKeys, newTeamExternalKeys, mongo, config, clubResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// venues
	await updateVenueClubReferences(oldVenueExternalKeys, newVenueExternalKeys, mongo, config, clubResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Synchronizes SGO aggregation documents when club SGO memberships change.
 *
 * Compares old vs new SGO external keys to identify membership changes, then updates
 * the affected SGO aggregation documents to add/remove club references accordingly.
 * This maintains bidirectional consistency between club and SGO aggregations.
 *
 * @async
 * @param {string[]} oldSgoExternalKeys - Previous SGO keys from club aggregation
 * @param {string[]} newSgoExternalKeys - Current SGO keys from club aggregation
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} clubResourceReference - Club reference object (resourceType, externalKey, objectId)
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<void>}
 */
async function updateSgoClubReferences(oldSgoExternalKeys, newSgoExternalKeys, mongo, config, clubResourceReference, requestId) {
	const sgoExternalKeysToRemoveClubFrom = oldSgoExternalKeys.filter((oldKey) => !newSgoExternalKeys.includes(oldKey));
	for (const oldSgoExternalKey of sgoExternalKeysToRemoveClubFrom) {
		const sgoAggregationDocToRemoveClubFrom = { resourceType: 'sgo', externalKey: { old: oldSgoExternalKey || null, new: null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, sgoAggregationDocToRemoveClubFrom, clubResourceReference, requestId);
	}
	const sgoExternalKeysToAddClubTo = newSgoExternalKeys.filter((newKey) => !oldSgoExternalKeys.includes(newKey));
	for (const newSgoExternalKey of sgoExternalKeysToAddClubTo) {
		const sgoAggregationDocToAddClubTo = { resourceType: 'sgo', externalKey: { old: null, new: newSgoExternalKey || null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, sgoAggregationDocToAddClubTo, clubResourceReference, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Synchronizes team aggregation documents when club team memberships change.
 *
 * Compares old vs new team external keys to identify membership changes, then updates
 * the affected team aggregation documents to add/remove club references accordingly.
 * This maintains bidirectional consistency between club and team aggregations.
 *
 * @async
 * @param {string[]} oldTeamExternalKeys - Previous team keys from club aggregation
 * @param {string[]} newTeamExternalKeys - Current team keys from club aggregation
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} clubResourceReference - Club reference object (resourceType, externalKey, objectId)
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<void>}
 */
async function updateTeamClubReferences(oldTeamExternalKeys, newTeamExternalKeys, mongo, config, clubResourceReference, requestId) {
	const teamExternalKeysToRemoveClubFrom = oldTeamExternalKeys.filter((oldKey) => !newTeamExternalKeys.includes(oldKey));
	for (const oldTeamExternalKey of teamExternalKeysToRemoveClubFrom) {
		const teamAggregationDocToRemoveClubFrom = { resourceType: 'team', externalKey: { old: oldTeamExternalKey || null, new: null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, teamAggregationDocToRemoveClubFrom, clubResourceReference, requestId);
	}
	const teamExternalKeysToAddClubTo = newTeamExternalKeys.filter((newKey) => !oldTeamExternalKeys.includes(newKey));
	for (const newTeamExternalKey of teamExternalKeysToAddClubTo) {
		const teamAggregationDocToAddClubTo = { resourceType: 'team', externalKey: { old: null, new: newTeamExternalKey || null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, teamAggregationDocToAddClubTo, clubResourceReference, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Synchronizes venue aggregation documents when club venue memberships change.
 *
 * Compares old vs new venue external keys to identify membership changes, then updates
 * the affected venue aggregation documents to add/remove club references accordingly.
 * This maintains bidirectional consistency between club and venue aggregations.
 *
 * @async
 * @param {string[]} oldVenueExternalKeys - Previous venue keys from club aggregation
 * @param {string[]} newVenueExternalKeys - Current venue keys from club aggregation
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} clubResourceReference - Club reference object (resourceType, externalKey, objectId)
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<void>}
 */
async function updateVenueClubReferences(oldVenueExternalKeys, newVenueExternalKeys, mongo, config, clubResourceReference, requestId) {
	const venueExternalKeysToRemoveClubFrom = oldVenueExternalKeys.filter((oldKey) => !newVenueExternalKeys.includes(oldKey));
	for (const oldVenueExternalKey of venueExternalKeysToRemoveClubFrom) {
		const venueAggregationDocToRemoveClubFrom = { resourceType: 'venue', externalKey: { old: oldVenueExternalKey || null, new: null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, venueAggregationDocToRemoveClubFrom, clubResourceReference, requestId);
	}
	const venueExternalKeysToAddClubTo = newVenueExternalKeys.filter((newKey) => !oldVenueExternalKeys.includes(newKey));
	for (const newVenueExternalKey of venueExternalKeysToAddClubTo) {
		const venueAggregationDocToAddClubTo = { resourceType: 'venue', externalKey: { old: null, new: newVenueExternalKey || null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, venueAggregationDocToAddClubTo, clubResourceReference, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processClub };
