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
	const newClubExternalKey = newAggregationDoc?.clubKeys[0];
	const clubAggregationDoc = { resourceType: 'club', externalKey: { old: oldClubExternalKey || null, new: newClubExternalKey || null } };
	await updateResourceReferencesInAggregationDocs(mongo, config, clubAggregationDoc, teamResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// _externalNationId
	const newNationExternalKey = newAggregationDoc?.nationKeys?.[0];
	const nationAggregationDoc = { resourceType: 'nation', externalKey: { old: oldNationExternalKey || null, new: newNationExternalKey || null } };
	await updateResourceReferencesInAggregationDocs(mongo, config, nationAggregationDoc, teamResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// _externalVenueId
	const newVenueExternalKey = newAggregationDoc?.venueKeys?.[0];
	const venueAggregationDoc = { resourceType: 'venue', externalKey: { old: oldVenueExternalKey || null, new: newVenueExternalKey || null } };
	await updateResourceReferencesInAggregationDocs(mongo, config, venueAggregationDoc, teamResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Sports Person IDs
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

	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processTeam };
