///////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { keySeparator } = require('../constants');
const { updateResourceReferencesInAggregationDocs } = require('../updateResourceReferencesInAggregationDocs');
const { pipeline } = require('./teamAggregationPipeline');
const { queryForTeamAggregationDoc } = require('./teamAggregationPipeline');

////////////////////////////////////////////////////////////////////////////////
// Process team updates
/**
 * Builds or rebuilds team aggregation documents and synchronizes cross-references.
 *
 * This function performs a complete aggregation build for a team, capturing all
 * related resources (clubs, sports persons, nations, venues) and maintaining referential
 * integrity across the materialized aggregation collection.
 *
 * Process flow:
 * 1. Validates team existence to avoid expensive pipeline execution on missing data
 * 2. Captures current aggregation state (old keys) before rebuilding
 * 3. Executes aggregation pipeline to rebuild team materialized view
 * 4. Compares old vs new keys to identify relationship changes
 * 5. Updates cross-references in related resource aggregations:
 *    - Club aggregations (add/remove team reference for club changes)
 *    - Nation aggregations (add/remove team reference for nation changes)
 *    - Venue aggregations (add/remove team reference for venue changes)
 *    - Sports person aggregations (add/remove team reference for member changes)
 *
 * The function ensures that when a team changes its club/nation/venue associations or
 * member roster, all affected aggregation documents are updated to maintain consistency.
 *
 * @async
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {string} teamIdScope - External scope identifier for the team
 * @param {string} teamId - External identifier for the team
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<Object|number>} New aggregation document or 404 if team not found
 * @throws {Error} If configuration is invalid or required parameters are missing
 */
async function processTeam(config, mongo, teamIdScope, teamId, requestId) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!teamId || !teamIdScope) throw new Error('Invalid parameters: teamId and teamIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`teamIdScope=${teamIdScope}, teamId=${teamId}`, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Check if team exists before running expensive pipeline
	const teamExists = await mongo.db.collection('teams').countDocuments({ _externalId: teamId, _externalIdScope: teamIdScope }, { limit: 1 });
	//////////////////////////////////////////////////////////////////////////////
	if (teamExists === 0) {
		debug(`Team not found: ${teamId}@${teamIdScope}`, requestId);
		return 404;
	}
	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, teamIdScope, teamId);
	const teamAggregationDocQuery = queryForTeamAggregationDoc(teamId, teamIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the previous version of the team aggregation (if it exists) and calculate old keys
	// A team has outbound references to:
	// - Club
	// - SportsPersons (via members)
	// - Nation
	// - Venue
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(teamAggregationDocQuery);
	const oldClubExternalKey = oldAggregationDoc?.competitionKeys?.[0];
	const oldSportsPersonExternalKeys = oldAggregationDoc?.sportsPersonKeys || [];
	const oldNationExternalKey = oldAggregationDoc?.nationKeys?.[0];
	const oldVenueExternalKey = oldAggregationDoc?.venueKeys?.[0];
	//////////////////////////////////////////////////////////////////////////////
	// Build the team aggregation view
	await runPipeline(mongo, 'teams', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the teams aggregation and calculate new outbound keys
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(teamAggregationDocQuery);
	//////////////////////////////////////////////////////////////////////////////
	// FIX OUTBOUND REFERENCES
	const teamObjectId = newAggregationDoc?.gamedayId;
	const teamKey = `${teamId}${keySeparator}${teamIdScope}`;
	const teamResourceReference = { resourceType: 'team', externalKey: teamKey, objectId: teamObjectId };
	//////////////////////////////////////////////////////////////////////////////
	// _externalClubId
	await updateClubReferences(newAggregationDoc, oldClubExternalKey, mongo, config, teamResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// _externalNationId
	await updateNationReferences(newAggregationDoc, oldNationExternalKey, mongo, config, teamResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// _externalVenueId
	await updateVenueReferences(newAggregationDoc, oldVenueExternalKey, mongo, config, teamResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Sports Person IDs
	await updateSportsPersonReferences(newAggregationDoc, oldSportsPersonExternalKeys, mongo, config, teamResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Updates sports person aggregation documents when a team's sports person membership changes.
 *
 * Compares old vs new sports person external keys to identify membership changes, then updates
 * the affected sports person aggregation documents to add/remove team references accordingly.
 * This maintains bidirectional consistency between team and sports person aggregations.
 *
 * Process:
 * 1. Identifies sports persons losing this team (in old but not new keys)
 * 2. Removes team reference from those sports person aggregations
 * 3. Identifies sports persons gaining this team (in new but not old keys)
 * 4. Adds team reference to those sports person aggregations
 *
 * @async
 * @param {Object} newAggregationDoc - Updated team aggregation document
 * @param {string[]} oldSportsPersonExternalKeys - Previous sports person keys from team aggregation
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} teamResourceReference - Team reference object (resourceType, externalKey, objectId)
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<void>}
 */
async function updateSportsPersonReferences(newAggregationDoc, oldSportsPersonExternalKeys, mongo, config, teamResourceReference, requestId) {
	const newSportsPersonExternalKeys = newAggregationDoc?.sportsPersonKeys || [];
	// Create a set of keys that are in oldSportsPersonExternalKeys but not in newSportsPersonExternalKeys
	// This team then needs to be removed all sports person aggregation docs for this set
	const sportsPersonExternalKeysToRemoveTeamFrom = oldSportsPersonExternalKeys.filter((oldKey) => !newSportsPersonExternalKeys.includes(oldKey));
	for (const oldSportsPersonExternalKey of sportsPersonExternalKeysToRemoveTeamFrom) {
		const sportsPersonAggregationDocToRemoveTeamFrom = { resourceType: 'sportsPerson', externalKey: { old: oldSportsPersonExternalKey || null, new: null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, sportsPersonAggregationDocToRemoveTeamFrom, teamResourceReference, requestId);
	}
	// Create a set of keys that are in newSportsPersonExternalKeys but not in oldSportsPersonExternalKeys
	// This team then needs to be added to all sports person aggregation docs for this set
	const sportsPersonExternalKeysToAddTeamTo = newSportsPersonExternalKeys.filter((newKey) => !oldSportsPersonExternalKeys.includes(newKey));
	for (const newSportsPersonExternalKey of sportsPersonExternalKeysToAddTeamTo) {
		const sportsPersonAggregationDocToAddTeamTo = { resourceType: 'sportsPerson', externalKey: { old: null, new: newSportsPersonExternalKey || null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, sportsPersonAggregationDocToAddTeamTo, teamResourceReference, requestId);
	}
}
////////////////////////////////////////////////////////////////////////////////
/**
 * Updates venue aggregation documents when a team's venue association changes.
 *
 * Compares the old vs new venue external keys to determine if the team has moved
 * between venues, then updates the affected venue aggregation documents to
 * add/remove the team reference accordingly. This maintains bidirectional consistency
 * between team and venue aggregations.
 *
 * The function delegates to updateResourceReferencesInAggregationDocs to handle the
 * actual addition/removal of team references from venue aggregation documents.
 *
 * @async
 * @param {Object} newAggregationDoc - Updated team aggregation document
 * @param {string|null} oldVenueExternalKey - Previous venue key from team aggregation
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} teamResourceReference - Team reference object (resourceType, externalKey, objectId)
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<void>}
 */
async function updateVenueReferences(newAggregationDoc, oldVenueExternalKey, mongo, config, teamResourceReference, requestId) {
	const newVenueExternalKey = newAggregationDoc?.venueKeys?.[0];
	const venueAggregationDoc = { resourceType: 'venue', externalKey: { old: oldVenueExternalKey || null, new: newVenueExternalKey || null } };
	await updateResourceReferencesInAggregationDocs(mongo, config, venueAggregationDoc, teamResourceReference, requestId);
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Updates nation aggregation documents when a team's nation association changes.
 *
 * Compares the old vs new nation external keys to determine if the team has moved
 * between nations, then updates the affected nation aggregation documents to
 * add/remove the team reference accordingly. This maintains bidirectional consistency
 * between team and nation aggregations.
 *
 * The function delegates to updateResourceReferencesInAggregationDocs to handle the
 * actual addition/removal of team references from nation aggregation documents.
 *
 * @async
 * @param {Object} newAggregationDoc - Updated team aggregation document
 * @param {string|null} oldNationExternalKey - Previous nation key from team aggregation
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} teamResourceReference - Team reference object (resourceType, externalKey, objectId)
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<void>}
 */
async function updateNationReferences(newAggregationDoc, oldNationExternalKey, mongo, config, teamResourceReference, requestId) {
	const newNationExternalKey = newAggregationDoc?.nationKeys?.[0];
	const nationAggregationDoc = { resourceType: 'nation', externalKey: { old: oldNationExternalKey || null, new: newNationExternalKey || null } };
	await updateResourceReferencesInAggregationDocs(mongo, config, nationAggregationDoc, teamResourceReference, requestId);
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Updates club aggregation documents when a team's club association changes.
 *
 * Compares the old vs new club external keys to determine if the team has moved
 * between clubs, then updates the affected club aggregation documents to
 * add/remove the team reference accordingly. This maintains bidirectional consistency
 * between team and club aggregations.
 *
 * The function delegates to updateResourceReferencesInAggregationDocs to handle the
 * actual addition/removal of team references from club aggregation documents.
 *
 * @async
 * @param {Object} newAggregationDoc - Updated team aggregation document
 * @param {string|null} oldClubExternalKey - Previous club key from team aggregation
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} teamResourceReference - Team reference object (resourceType, externalKey, objectId)
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<void>}
 */
async function updateClubReferences(newAggregationDoc, oldClubExternalKey, mongo, config, teamResourceReference, requestId) {
	const newClubExternalKey = newAggregationDoc?.clubKeys[0];
	const clubAggregationDoc = { resourceType: 'club', externalKey: { old: oldClubExternalKey || null, new: newClubExternalKey || null } };
	await updateResourceReferencesInAggregationDocs(mongo, config, clubAggregationDoc, teamResourceReference, requestId);
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processTeam };
