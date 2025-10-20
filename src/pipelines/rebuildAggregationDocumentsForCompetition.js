const _ = require('lodash');
const { debug, info, warn } = require('../log.js');
const separators = require('./constants.js');
const { processCompetition } = require('./competition/competitionAggregationBuild.js');
const { processStage } = require('./stage/stageAggregationBuild.js');
const { processEvent } = require('./event/eventAggregationBuild.js');
const { processSgo } = require('./sgo/sgoAggregationBuild.js');
const { processVenue } = require('./venue/venueAggregationBuild.js');
const { processClub } = require('./club/clubAggregationBuild.js');
const { processTeam } = require('./team/teamAggregationBuild.js');
const { processStaff } = require('./staff/staffAggregationBuild.js');
const { processRanking } = require('./ranking/rankingAggregationBuild.js');
const { processSportsPerson } = require('./sportsPerson/sportsPersonAggregationBuild.js');
const { processNation } = require('./nation/nationAggregationBuild.js');
const { RankingKeyClass } = require('./ranking/rankingKeyClass.js');

////////////////////////////////////////////////////////////////////////////////
// Helper function to extract keys from aggregation document and add to rebuild sets
function collectKeysForRebuild(aggDoc, collectorsMap) {
	if (!_.isObject(aggDoc)) return;
	for (const [keyType, collector] of Object.entries(collectorsMap)) {
		const keys = Object.keys(aggDoc[keyType] || {});
		keys.forEach((key) => collector.add(key));
	}
}

////////////////////////////////////////////////////////////////////////////////
// Generic rebuild function for a single resource type
async function rebuildResourceType(mongo, config, resourceType, keysToRebuild, trackingSets, requestId, onSuccess = null) {
	const { attemptedRebuildSet, completedRebuildSet, failedRebuildSet, skippedRebuildSet } = trackingSets;
	info(`Starting full rebuild of ${resourceType} aggregation references`, requestId);
	count = 1;
	const lengthOfKeys = Array.isArray(keysToRebuild) ? keysToRebuild.length : _.isSet(keysToRebuild) ? keysToRebuild.size : `unknown`;
	for (const key of keysToRebuild) {
		const resourceId = `{ type: ${resourceType}, key: ${key} }`;
		attemptedRebuildSet.add(resourceId);
		const aggDoc = await buildAggregationDocument(mongo, config, resourceType, key, requestId);
		if (aggDoc == null) {
			failedRebuildSet.add(resourceId);
			warn(`Failed ${resourceType}: ${count}/${lengthOfKeys} ${reportNumbers(trackingSets)}`, requestId);
			continue;
		}
		if (aggDoc === 500) {
			skippedRebuildSet.add(resourceId);
			warn(`Unsupported rebuild ${resourceType}: ${count}/${lengthOfKeys} - skipping. ${reportNumbers(trackingSets)}`, requestId);
			continue;
		}
		if (_.isObject(aggDoc)) {
			completedRebuildSet.add(resourceId);
			info(`Rebuild ${resourceType}: ${count}/${lengthOfKeys} ${reportNumbers(trackingSets)}`, requestId);
			if (onSuccess && typeof onSuccess === 'function') onSuccess(aggDoc);
		}
		count++;
	}
}

////////////////////////////////////////////////////////////////////////////////
// Helper function to create tracking sets
function createTrackingSets() {
	return {
		attemptedRebuildSet: new Set(),
		completedRebuildSet: new Set(),
		failedRebuildSet: new Set(),
		skippedRebuildSet: new Set(),
	};
}

////////////////////////////////////////////////////////////////////////////////
// Helper function to create rebuild collectors
function createRebuildCollectors() {
	return {
		clubToRebuild: new Set(),
		eventsToRebuild: new Set(),
		keyMomentsToRebuild: new Set(),
		nationsToRebuild: new Set(),
		rankingsToRebuild: new Set(),
		sportsPersonsToRebuild: new Set(),
		staffToRebuild: new Set(),
		teamsToRebuild: new Set(),
		venuesToRebuild: new Set(),
	};
}

////////////////////////////////////////////////////////////////////////////////
function reportNumbers(trackingSets) {
	const { attemptedRebuildSet, completedRebuildSet, failedRebuildSet, skippedRebuildSet } = trackingSets;
	return `[attempted: ${attemptedRebuildSet.size}, completed: ${completedRebuildSet.size}, failed: ${failedRebuildSet.size}, skipped: ${skippedRebuildSet.size}]`;
}

////////////////////////////////////////////////////////////////////////////////
// Specific rebuild functions for each resource type
async function rebuildStages(mongo, config, competitionAggregationDoc, trackingSets, collectors, requestId) {
	const stageKeys = Object.keys(competitionAggregationDoc.stageKeys || {});
	await rebuildResourceType(mongo, config, 'stage', stageKeys, trackingSets, requestId, (stageAggDoc) => {
		collectKeysForRebuild(stageAggDoc, {
			eventKeys: collectors.eventsToRebuild,
			rankingKeys: collectors.rankingsToRebuild,
		});
	});
}

////////////////////////////////////////////////////////////////////////////////
async function rebuildEvents(mongo, config, collectors, trackingSets, requestId) {
	await rebuildResourceType(mongo, config, 'event', collectors.eventsToRebuild, trackingSets, requestId, (eventAggDoc) => {
		collectKeysForRebuild(eventAggDoc, {
			venueKeys: collectors.venuesToRebuild,
			teamKeys: collectors.teamsToRebuild,
			sportsPersonKeys: collectors.sportsPersonsToRebuild,
		});
		// Note: Fixed bug - was using stageAggDoc instead of eventAggDoc
		collectKeysForRebuild(eventAggDoc, {
			rankingKeys: collectors.rankingsToRebuild,
			keyMomentKeys: collectors.keyMomentsToRebuild,
		});
	});
}

////////////////////////////////////////////////////////////////////////////////
async function rebuildRankings(mongo, config, collectors, trackingSets, requestId) {
	await rebuildResourceType(mongo, config, 'ranking', collectors.rankingsToRebuild, trackingSets, requestId, (rankingAggDoc) => {
		collectKeysForRebuild(rankingAggDoc, {
			teamKeys: collectors.teamsToRebuild,
			sportsPersonKeys: collectors.sportsPersonsToRebuild,
		});
	});
}

////////////////////////////////////////////////////////////////////////////////
async function rebuildTeams(mongo, config, collectors, trackingSets, requestId) {
	await rebuildResourceType(mongo, config, 'team', collectors.teamsToRebuild, trackingSets, requestId, (teamAggDoc) => {
		collectKeysForRebuild(teamAggDoc, {
			staffKeys: collectors.staffToRebuild,
			clubKeys: collectors.clubToRebuild,
			sportsPersonKeys: collectors.sportsPersonsToRebuild,
			nationKeys: collectors.nationsToRebuild,
			venueKeys: collectors.venuesToRebuild,
		});
	});
}

////////////////////////////////////////////////////////////////////////////////
async function rebuildStaff(mongo, config, collectors, trackingSets, requestId) {
	await rebuildResourceType(mongo, config, 'staff', collectors.staffToRebuild, trackingSets, requestId, (staffAggDoc) => {
		collectKeysForRebuild(staffAggDoc, {
			clubKeys: collectors.clubToRebuild,
			sportsPersonKeys: collectors.sportsPersonsToRebuild,
			nationKeys: collectors.nationsToRebuild,
		});
	});
}

////////////////////////////////////////////////////////////////////////////////
// Simple rebuild functions (no key collection needed)
async function rebuildSportsPersons(mongo, config, collectors, trackingSets, requestId) {
	await rebuildResourceType(mongo, config, 'sportsPerson', collectors.sportsPersonsToRebuild, trackingSets, requestId);
}

async function rebuildClubs(mongo, config, collectors, trackingSets, requestId) {
	await rebuildResourceType(mongo, config, 'club', collectors.clubToRebuild, trackingSets, requestId);
}

async function rebuildNations(mongo, config, collectors, trackingSets, requestId) {
	await rebuildResourceType(mongo, config, 'nation', collectors.nationsToRebuild, trackingSets, requestId);
}

async function rebuildVenues(mongo, config, collectors, trackingSets, requestId) {
	await rebuildResourceType(mongo, config, 'venue', collectors.venuesToRebuild, trackingSets, requestId);
}

////////////////////////////////////////////////////////////////////////////////
// Main function - now much smaller and cleaner
async function rebuildAggregationDocumentsForCompetition(mongo, config, competitionAggregationDocument, requestId) {
	//////////////////////////////////////////////////////////////////////////////
	// Validate
	if (!_.isObject(competitionAggregationDocument)) throw new Error('Invalid parameters: competitionAggregationDocument must be an object');
	if (!_.isString(competitionAggregationDocument.resourceType)) throw new Error('Invalid parameters: competitionAggregationDocument.resourceType must be a string');
	if (!_.isString(competitionAggregationDocument.externalKey)) throw new Error('Invalid parameters: competitionAggregationDocument.externalKey must be a string');
	if (competitionAggregationDocument.resourceType !== 'competition')
		throw new Error('Invalid parameters: rebuildAggregationDocumentsForCompetition can only process competition aggregation documents as the root');
	const thisId = `{ type: ${competitionAggregationDocument.resourceType}, key: ${competitionAggregationDocument.externalKey} }`;
	const trackingSets = createTrackingSets();
	const collectors = createRebuildCollectors();
	try {
		////////////////////////////////////////////////////////////////////////////
		// Process SGOs using existing recursive function
		info(`Starting full rebuild of SGO aggregation references for competition ${thisId}`, requestId);
		await processAggregationDocument(
			mongo,
			config,
			competitionAggregationDocument,
			requestId,
			trackingSets.attemptedRebuildSet,
			trackingSets.completedRebuildSet,
			trackingSets.failedRebuildSet,
			trackingSets.skippedRebuildSet,
			['sgo']
		);

		////////////////////////////////////////////////////////////////////////////
		// Rebuild each resource type in dependency order
		await rebuildStages(mongo, config, competitionAggregationDocument, trackingSets, collectors, requestId);
		await rebuildEvents(mongo, config, collectors, trackingSets, requestId);
		await rebuildRankings(mongo, config, collectors, trackingSets, requestId);
		await rebuildTeams(mongo, config, collectors, trackingSets, requestId);
		await rebuildStaff(mongo, config, collectors, trackingSets, requestId);
		await rebuildSportsPersons(mongo, config, collectors, trackingSets, requestId);
		await rebuildClubs(mongo, config, collectors, trackingSets, requestId);
		await rebuildNations(mongo, config, collectors, trackingSets, requestId);
		await rebuildVenues(mongo, config, collectors, trackingSets, requestId);

		info(`Done rebuilding aggregation references for competition ${thisId}. ${reportNumbers(trackingSets)}`, requestId);
	} catch (error) {
		warn(`Error during full rebuild for competition ${thisId}: ${error.message}`, requestId);
		throw error;
	}
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Parses a simple key (id@scope format) by splitting it using the configured key separator.
 *
 * @param {string} key - The key string to be parsed
 * @returns {string[]} An array of key segments split by the separator
 *
 * @example
 * // If separators.keySeparator is ' @ '
 * parseSimpleKey('id @ scope')
 * // Returns: ['id', ' @ ', 'scope']
 */
function parseSimpleKey(key) {
	return key.split(separators.keySeparator);
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Parses a ranking key string to extract structured data components.
 *
 * The key format follows a specific pattern with separators to identify:
 * - Entity ID and scope (stage/event)
 * - Subject ID and scope (team/sports person)
 * - Date/time label
 * - Ranking position
 *
 * @param {string} key - The ranking key string to parse
 * @param {string} requestId - Request identifier for logging purposes
 * @returns {Object|null} Parsed ranking data object containing:
 *   - sportsPersonId: ID of the sports person (if applicable)
 *   - sportsPersonIdScope: Scope for sports person ID
 *   - teamId: ID of the team (if applicable)
 *   - teamIdScope: Scope for team ID
 *   - stageId: ID of the stage (if applicable)
 *   - stageIdScope: Scope for stage ID
 *   - eventId: ID of the event (if applicable)
 *   - eventIdScope: Scope for event ID
 *   - dateTimeLabel: Date/time label from the key
 *   - ranking: Numeric ranking position
 * Returns null if key format is invalid.
 */
function parseRankingKey(key, requestId) {
	const {
		keySeparator,
		rankingPositionSeparator,
		rankingLabelSeparator,
		rankingStageTeamSeparator,
		rankingEventTeamSeparator,
		rankingStageSportsPersonSeparator,
		rankingEventSportsPersonSeparator,
	} = separators;

	const escapeRegex = (str) => str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
	const keyRegexString = `^(.*)(${escapeRegex(keySeparator)})(.*)(${escapeRegex(rankingStageTeamSeparator)}|${escapeRegex(rankingEventTeamSeparator)}|${escapeRegex(
		rankingStageSportsPersonSeparator
	)}|${escapeRegex(rankingEventSportsPersonSeparator)})(.*)(${escapeRegex(keySeparator)})(.*)(${escapeRegex(rankingLabelSeparator)})(.*)(${escapeRegex(
		rankingPositionSeparator
	)})(.*)$`;

	const regex = new RegExp(keyRegexString);
	const keyParts = key.match(regex);

	if (!keyParts || keyParts.length !== 12) {
		warn(`Invalid ranking key format: ${key}`, requestId);
		return null;
	}

	const sportsPersonId = keyParts[4] === rankingStageSportsPersonSeparator || keyParts[4] === rankingEventSportsPersonSeparator ? keyParts[5] : null;
	const sportsPersonIdScope = sportsPersonId != null ? keyParts[7] : null;
	const teamId = keyParts[4] === rankingStageTeamSeparator || keyParts[4] === rankingEventTeamSeparator ? keyParts[5] : null;
	const teamIdScope = teamId != null ? keyParts[7] : null;
	const stageId = keyParts[4] === rankingStageSportsPersonSeparator || keyParts[4] === rankingStageTeamSeparator ? keyParts[1] : null;
	const stageIdScope = stageId != null ? keyParts[3] : null;
	const eventId = keyParts[4] === rankingEventSportsPersonSeparator || keyParts[4] === rankingEventTeamSeparator ? keyParts[1] : null;
	const eventIdScope = eventId != null ? keyParts[3] : null;
	const dateTimeLabel = keyParts[9];
	const ranking = _.isNaN(Number(keyParts[11])) ? null : Number(keyParts[11]);

	return {
		sportsPersonId,
		sportsPersonIdScope,
		teamId,
		teamIdScope,
		stageId,
		stageIdScope,
		eventId,
		eventIdScope,
		dateTimeLabel,
		ranking,
	};
}

////////////////////////////////////////////////////////////////////////////////
function parseStaffKey(key, requestId) {
	if (!_.isString(key)) {
		warn(`Invalid staff key format (not a string): ${key}`, requestId);
		return null;
	}
	const { keySeparator, teamSeparator, clubSeparator, nationSeparator } = separators;
	const escapeRegex = (str) => str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
	const keyRegexString = `^(.*)(${escapeRegex(keySeparator)})(.*)(${escapeRegex(teamSeparator)}|${escapeRegex(clubSeparator)}|${escapeRegex(
		nationSeparator
	)})(.*)(${escapeRegex(keySeparator)})(.*)$`; // 7 capturing groups
	const regex = new RegExp(keyRegexString);
	const keyParts = key.match(regex);
	if (!keyParts || keyParts.length !== 8) {
		warn(`Invalid staff key format: ${key}`, requestId);
		return null;
	}
	const sportsPersonId = keyParts[1];
	const sportsPersonIdScope = keyParts[3];
	const teamId = keyParts[4] === teamSeparator ? keyParts[5] : null;
	const teamIdScope = teamId != null ? keyParts[7] : null;
	const clubId = keyParts[4] === clubSeparator ? keyParts[5] : null;
	const clubIdScope = clubId != null ? keyParts[7] : null;
	const nationId = keyParts[4] === nationSeparator ? keyParts[5] : null;
	const nationIdScope = nationId != null ? keyParts[7] : null;
	if (
		(teamId == null && clubId == null && nationId == null) ||
		(teamId != null && clubId != null) ||
		(teamId != null && nationId != null) ||
		(clubId != null && nationId != null) ||
		(teamIdScope == null && clubIdScope == null && nationIdScope == null) ||
		(teamIdScope != null && clubIdScope != null) ||
		(teamIdScope != null && nationIdScope != null) ||
		(clubIdScope != null && nationIdScope != null)
	) {
		warn(`Invalid staff key format (must have exactly one of team, club, or nation): ${key}`, requestId);
		return null;
	}
	return {
		sportsPersonId,
		sportsPersonIdScope,
		teamId,
		teamIdScope,
		clubId,
		clubIdScope,
		nationId,
		nationIdScope,
	};
}

////////////////////////////////////////////////////////////////////////////////
async function rebuildSgoAggregationDocument(config, mongo, key, requestId) {
	const idParts = parseSimpleKey(key);
	debug(`Rebuilding SGO aggregation for ${key}`, requestId);
	return await processSgo(config, mongo, idParts[1], idParts[0], requestId, false);
}

////////////////////////////////////////////////////////////////////////////////
async function rebuildCompetitionAggregationDocument(config, mongo, key, requestId) {
	const idParts = parseSimpleKey(key);
	debug(`Rebuilding competition aggregation for ${idParts[0]}@${idParts[1]}`, requestId);
	return await processCompetition(config, mongo, idParts[1], idParts[0], requestId, false);
}

////////////////////////////////////////////////////////////////////////////////
async function rebuildStageAggregationDocument(config, mongo, key, requestId) {
	const idParts = parseSimpleKey(key);
	debug(`Rebuilding stage aggregation for ${key}`, requestId);
	return await processStage(config, mongo, idParts[1], idParts[0], requestId, false);
}

////////////////////////////////////////////////////////////////////////////////
async function rebuildEventAggregationDocument(config, mongo, key, requestId) {
	const idParts = parseSimpleKey(key);
	debug(`Rebuilding event aggregation for ${key}`, requestId);
	return await processEvent(config, mongo, idParts[1], idParts[0], requestId, false);
}

////////////////////////////////////////////////////////////////////////////////
async function rebuildVenueAggregationDocument(config, mongo, key, requestId) {
	const idParts = parseSimpleKey(key);
	debug(`Rebuilding Venue aggregation for ${key}`, requestId);
	return await processVenue(config, mongo, idParts[1], idParts[0], requestId, false);
}

////////////////////////////////////////////////////////////////////////////////
async function rebuildClubAggregationDocument(config, mongo, key, requestId) {
	const idParts = parseSimpleKey(key);
	debug(`Rebuilding Club aggregation for ${key}`, requestId);
	return await processClub(config, mongo, idParts[1], idParts[0], requestId, false);
}

////////////////////////////////////////////////////////////////////////////////
async function rebuildTeamAggregationDocument(config, mongo, key, requestId) {
	const idParts = parseSimpleKey(key);
	debug(`Rebuilding Team aggregation for ${key}`, requestId);
	return await processTeam(config, mongo, idParts[1], idParts[0], requestId, false);
}

////////////////////////////////////////////////////////////////////////////////
async function rebuildSportsPersonAggregationDocument(config, mongo, key, requestId) {
	const idParts = parseSimpleKey(key);
	debug(`Rebuilding SportsPerson aggregation for ${key}`, requestId);
	return await processSportsPerson(config, mongo, idParts[1], idParts[0], requestId, false);
}

////////////////////////////////////////////////////////////////////////////////
async function rebuildNationAggregationDocument(config, mongo, key, requestId) {
	const idParts = parseSimpleKey(key);
	debug(`Rebuilding Nation aggregation for ${key}`, requestId);
	return await processNation(config, mongo, idParts[1], idParts[0], requestId, false);
}

////////////////////////////////////////////////////////////////////////////////
async function rebuildRankingAggregationDocument(config, mongo, key, requestId) {
	const rankingParams = parseRankingKey(key, requestId);
	if (!rankingParams) {
		return null;
	}

	const rankingKeyClass = new RankingKeyClass(rankingParams);
	if (!rankingKeyClass.validate()) {
		warn(`Invalid ranking key components: ${key}`, requestId);
		return null;
	}

	debug(`Rebuilding Ranking aggregation for ${key}`, requestId);
	return await processRanking(config, mongo, rankingKeyClass, requestId, false);
}

////////////////////////////////////////////////////////////////////////////////
async function rebuildStaffAggregationDocument(config, mongo, key, requestId) {
	const staffParams = parseStaffKey(key, requestId);
	if (!staffParams) {
		return null;
	}
	debug(`Rebuilding Staff aggregation for ${key}`, requestId);
	const { sportsPersonId, sportsPersonIdScope, teamId, teamIdScope, clubId, clubIdScope, nationId, nationIdScope } = staffParams;
	return await processStaff(config, mongo, sportsPersonId, sportsPersonIdScope, teamId, teamIdScope, clubId, clubIdScope, nationId, nationIdScope, requestId, false);
}

////////////////////////////////////////////////////////////////////////////////
const REBUILD_HANDLERS = {
	club: rebuildClubAggregationDocument,
	competition: rebuildCompetitionAggregationDocument,
	event: rebuildEventAggregationDocument,
	//keyMoment
	nation: rebuildNationAggregationDocument,
	ranking: rebuildRankingAggregationDocument,
	sgo: rebuildSgoAggregationDocument,
	sportsPerson: rebuildSportsPersonAggregationDocument,
	staff: rebuildStaffAggregationDocument,
	stage: rebuildStageAggregationDocument,
	team: rebuildTeamAggregationDocument,
	venue: rebuildVenueAggregationDocument,
};

////////////////////////////////////////////////////////////////////////////////
async function buildAggregationDocument(mongo, config, type, key, requestId) {
	if (!_.isString(type)) throw new Error('Invalid parameters: type must be a string');
	if (!_.isString(key)) throw new Error('Invalid parameters: key must be a string');

	const rebuildHandler = REBUILD_HANDLERS[type];
	if (!rebuildHandler) {
		debug(`Unsupported aggregation type: ${type}`, requestId);
		return 500;
	}
	try {
		return await rebuildHandler(config, mongo, key, requestId);
	} catch (error) {
		warn(`Error rebuilding ${type} aggregation for ${key}: ${error.message}`, requestId);
		return null;
	}
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Recursively processes an aggregation document by rebuilding all referenced aggregation documents.
 * Tracks rebuild attempts, completions, failures, and skips to prevent duplicate processing.
 *
 * @async
 * @function processAggregationDocument
 * @param {Object} mongo - MongoDB connection instance
 * @param {Object} config - Configuration object for the aggregation process
 * @param {Object} newAggregationDoc - The aggregation document to process
 * @param {string} newAggregationDoc.resourceType - Type of the resource
 * @param {string} newAggregationDoc.externalKey - External key identifier
 * @param {string} requestId - Unique identifier for tracking the request
 * @param {Set} attemptedRebuildSet - Set tracking all attempted rebuilds
 * @param {Set} completedRebuildSet - Set tracking successfully completed rebuilds
 * @param {Set} failedRebuildSet - Set tracking failed rebuild attempts
 * @param {Set} skippedRebuildSet - Set tracking skipped rebuilds
 * @param {string[]} [limitToTypes=['sgo', 'competition', 'stage', 'event', 'team', 'club', 'nation', 'sportsPerson', 'venue', 'keyMoment', 'staff', 'ranking']] - Array of resource types to process
 * @returns {Promise<void>} Promise that resolves when processing is complete
 *
 * @description
 * This function iterates through all reference keys in the aggregation document for each specified type,
 * attempts to rebuild the referenced aggregation documents, and recursively processes any newly built
 * documents. It maintains state through Sets to prevent infinite loops and duplicate processing.
 *
 * The function handles three rebuild outcomes:
 * - null: Failed rebuild (added to failedRebuildSet)
 * - 500: Unsupported type (added to skippedRebuildSet)
 * - Object: Successful rebuild (added to completedRebuildSet and processed recursively)
 */
async function processAggregationDocument(
	mongo,
	config,
	newAggregationDoc,
	requestId,
	attemptedRebuildSet,
	completedRebuildSet,
	failedRebuildSet,
	skippedRebuildSet,
	limitToTypes = ['sgo', 'competition', 'stage', 'event', 'team', 'club', 'nation', 'sportsPerson', 'venue', 'keyMoment', 'staff', 'ranking']
) {
	if (!_.isObject(newAggregationDoc) || !_.isString(newAggregationDoc.resourceType) || !_.isString(newAggregationDoc.externalKey)) return;
	const thisId = `{ type: ${newAggregationDoc.resourceType}, key: ${newAggregationDoc.externalKey} }`;
	if (!_.isSet(attemptedRebuildSet)) attemptedRebuildSet = new Set();
	if (!_.isSet(completedRebuildSet)) completedRebuildSet = new Set();
	if (!_.isSet(failedRebuildSet)) failedRebuildSet = new Set();
	if (!_.isSet(skippedRebuildSet)) skippedRebuildSet = new Set();
	if (!attemptedRebuildSet.has(thisId)) attemptedRebuildSet.add(thisId);
	if (!completedRebuildSet.has(thisId)) completedRebuildSet.add(thisId);
	if (!_.isObject(newAggregationDoc)) newAggregationDoc = {};
	for (const rt of limitToTypes) {
		const newReferences = newAggregationDoc[`${rt}Keys`] || {};
		const newKeys = Object.keys(newReferences); // These will get us the ids of the objects to rebuild
		for (const newKey of newKeys) {
			const keyIdentifier = `{ type: ${rt}, key: ${newKey} }`;
			//////////////////////////////////////////////////////////////////////////
			// Do not repeat things already attempted
			if (attemptedRebuildSet.has(keyIdentifier)) {
				debug(`Skipping rebuild for ${keyIdentifier} - already attempted`, requestId);
				continue;
			} else {
				attemptedRebuildSet.add(keyIdentifier);
				debug(`Processing rebuild for ${keyIdentifier} from ${thisId}`, requestId);
			}

			//////////////////////////////////////////////////////////////////////////
			// Attempt to rebuild the referenced aggregation document
			const embeddedAggDoc = await buildAggregationDocument(mongo, config, rt, newKey, requestId);
			//////////////////////////////////////////////////////////////////////////
			// Track failures
			if (embeddedAggDoc == null) {
				failedRebuildSet.add(keyIdentifier);
				debug(
					`Failed rebuild of aggregation document for ${keyIdentifier}. Total rebuilds attempted: ${attemptedRebuildSet.size}, completed: ${completedRebuildSet.size}, failed: ${failedRebuildSet.size}, skipped: ${skippedRebuildSet.size}`,
					requestId
				);
				continue;
			}
			//////////////////////////////////////////////////////////////////////////
			// Skip over unsupported types, already added to attempted
			if (embeddedAggDoc === 500) {
				skippedRebuildSet.add(keyIdentifier);
				debug(
					`Unsupported rebuild of aggregation document for ${keyIdentifier} - skipping. Total rebuilds attempted: ${attemptedRebuildSet.size}, completed: ${completedRebuildSet.size}, failed: ${failedRebuildSet.size}, skipped: ${skippedRebuildSet.size}`,
					requestId
				);
				continue;
			}
			//////////////////////////////////////////////////////////////////////////
			// Mark as completed
			completedRebuildSet.add(keyIdentifier);
			debug(
				`Completed rebuild of aggregation document for ${keyIdentifier}. Total rebuilds attempted: ${attemptedRebuildSet.size}, completed: ${completedRebuildSet.size}, failed: ${failedRebuildSet.size}, skipped: ${skippedRebuildSet.size}`,
				requestId
			);
			//////////////////////////////////////////////////////////////////////////
			// Recursively process the newly rebuilt aggregation document
			await processAggregationDocument(mongo, config, embeddedAggDoc, requestId, attemptedRebuildSet, completedRebuildSet, failedRebuildSet, skippedRebuildSet, limitToTypes);
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { rebuildAggregationDocumentsForCompetition };
