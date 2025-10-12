///////////////////////////////////////////////////////////////////////////////
// When a competition is created or updated, we need to build its aggregated view
// We do not need to validate other materialized views as competitions are not
// referenced by other entities (e.g. events) in a way that requires updating the
// competition view when those entities change.
////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { keySeparator } = require('../constants');
const { updateResourceReferencesInAggregationDocs } = require('../updateResourceReferencesInAggregationDocs');
const { pipeline } = require('./competitionAggregationPipeline');
const { queryForCompetitionAggregationDoc } = require('./competitionAggregationPipeline');

////////////////////////////////////////////////////////////////////////////////
// Process competition updates
async function processCompetition(config, mongo, competitionIdScope, competitionId, requestId) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!competitionId || !competitionIdScope) throw new Error('Invalid parameters: competitionId and competitionIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`competitionIdScope=${competitionIdScope}, competitionId=${competitionId}`, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Check if competition exists before running expensive pipeline
	const competitionExists = await mongo.db.collection('competitions').countDocuments({ _externalId: competitionId, _externalIdScope: competitionIdScope }, { limit: 1 });
	//////////////////////////////////////////////////////////////////////////////
	if (competitionExists === 0) {
		debug(`Competition not found: ${competitionId}@${competitionIdScope}`, requestId);
		return 404;
	}
	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, competitionIdScope, competitionId);
	const competitionAggregationDocQuery = queryForCompetitionAggregationDoc(competitionId, competitionIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the previous version of the competition aggregation (if it exists) and calculate old sgo keys
	// A competition has outbound references to:
	// - SGOs (via sgoMemberships)
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(competitionAggregationDocQuery);
	const oldSgoExternalKeys = oldAggregationDoc?.sgoKeys || [];
	//////////////////////////////////////////////////////////////////////////////
	// Build the competition aggregation view
	await runPipeline(mongo, 'competitions', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the competition aggregation and calculate new competition keys
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(competitionAggregationDocQuery);
	const newSgoExternalKeys = newAggregationDoc?.sgoKeys || [];
	//////////////////////////////////////////////////////////////////////////////
	// FIX UPWARDS REFERENCES
	const competitionObjectId = newAggregationDoc?.gamedayId;
	const competitionKey = `${competitionId}${keySeparator}${competitionIdScope}`;
	const competitionResourceReference = { resourceType: 'competition', externalKey: competitionKey, objectId: competitionObjectId };
	//////////////////////////////////////////////////////////////////////////////
	// sgoMemberships._externalSgoId
	await updateSgoCompetitionReferences(oldSgoExternalKeys, newSgoExternalKeys, mongo, config, competitionResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Synchronizes SGO aggregation documents when competition SGO memberships change.
 *
 * Compares old vs new SGO external keys to identify membership changes, then updates
 * the affected SGO aggregation documents to add/remove competition references accordingly.
 * This maintains bidirectional consistency between competition and SGO aggregations.
 *
 * Process:
 * 1. Identifies SGOs losing this competition (in old but not new keys)
 * 2. Removes competition reference from those SGO aggregations
 * 3. Identifies SGOs gaining this competition (in new but not old keys)
 * 4. Adds competition reference to those SGO aggregations
 *
 * @async
 * @param {string[]} oldSgoExternalKeys - Previous SGO keys from competition aggregation
 * @param {string[]} newSgoExternalKeys - Current SGO keys from competition aggregation
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} competitionResourceReference - Competition reference object (resourceType, externalKey, objectId)
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<void>}
 */
async function updateSgoCompetitionReferences(oldSgoExternalKeys, newSgoExternalKeys, mongo, config, competitionResourceReference, requestId) {
	const sgoExternalKeysToRemoveCompetitionFrom = oldSgoExternalKeys.filter((oldKey) => !newSgoExternalKeys.includes(oldKey));
	for (const oldSgoExternalKey of sgoExternalKeysToRemoveCompetitionFrom) {
		const sgoAggregationDocToRemoveCompetitionFrom = { resourceType: 'sgo', externalKey: { old: oldSgoExternalKey || null, new: null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, sgoAggregationDocToRemoveCompetitionFrom, competitionResourceReference, requestId);
	}
	// Create a set of keys that are in newSgoExternalKeys but not in oldSgoExternalKeys
	// This competition then needs to be added to all SGO aggregation docs for this set
	const sgoExternalKeysToAddCompetitionTo = newSgoExternalKeys.filter((newKey) => !oldSgoExternalKeys.includes(newKey));
	for (const newSgoExternalKey of sgoExternalKeysToAddCompetitionTo) {
		const sgoAggregationDocToAddCompetitionTo = { resourceType: 'sgo', externalKey: { old: null, new: newSgoExternalKey || null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, sgoAggregationDocToAddCompetitionTo, competitionResourceReference, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processCompetition };
