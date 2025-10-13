////////////////////////////////////////////////////////////////////////////////
// Builds or rebuilds nation aggregation documents and updates inbound references
// (teams, venues, sgos) to maintain bidirectional consistency.
////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { keySeparator } = require('../constants');
const { updateResourceReferencesInAggregationDocs } = require('../updateResourceReferencesInAggregationDocs');
const { pipeline, queryForNationAggregationDoc } = require('./nationAggregationPipeline');

////////////////////////////////////////////////////////////////////////////////
/**
 * Process nation aggregation build.
 */
async function processNation(config, mongo, nationIdScope, nationId, requestId) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!nationId || !nationIdScope) throw new Error('Invalid parameters: nationId and nationIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`nationIdScope=${nationIdScope}, nationId=${nationId}`, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Validate nation exists
	const exists = await mongo.db.collection('nations').countDocuments({ _externalId: nationId, _externalIdScope: nationIdScope }, { limit: 1 });
	if (exists === 0) {
		debug(`Nation not found: ${nationId}@${nationIdScope}`, requestId);
		return 404;
	}
	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, nationIdScope, nationId);
	const nationAggQuery = queryForNationAggregationDoc(nationId, nationIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Previous aggregation (for diff)
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(nationAggQuery);
	const oldSgoExternalKeys = oldAggregationDoc?.sgoKeys || [];
	const oldTeamExternalKeys = oldAggregationDoc?.teamKeys || [];
	const oldVenueExternalKeys = oldAggregationDoc?.venueKeys || [];
	//////////////////////////////////////////////////////////////////////////////
	// Rebuild
	await runPipeline(mongo, 'nations', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the nation aggregation and calculate new reference keys
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(nationAggQuery);
	//////////////////////////////////////////////////////////////////////////////
	const newSgoExternalKeys = newAggregationDoc?.sgoKeys || [];
	const newTeamExternalKeys = newAggregationDoc?.teamKeys || [];
	const newVenueExternalKeys = newAggregationDoc?.venueKeys || [];
	//////////////////////////////////////////////////////////////////////////////
	// FIX REFERENCES
	const nationObjectId = newAggregationDoc?.gamedayId;
	const nationKey = `${nationId}${keySeparator}${nationIdScope}`;
	const nationResourceReference = { resourceType: 'nation', externalKey: nationKey, objectId: nationObjectId };
	//////////////////////////////////////////////////////////////////////////////
	// sgos
	await updateSgoNationReferences(oldSgoExternalKeys, newSgoExternalKeys, mongo, config, nationResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// teams
	await updateTeamNationReferences(oldTeamExternalKeys, newTeamExternalKeys, mongo, config, nationResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// venues
	await updateVenueNationReferences(oldVenueExternalKeys, newVenueExternalKeys, mongo, config, nationResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Updates references to team sgos in aggregation documents by comparing old and new keys.
 *
 * For each key removed from the oldKeys array, updates the aggregation documents to remove the reference.
 * For each key added in the newKeys array, updates the aggregation documents to add the reference.
 *
 * @async
 * @param {string[]} oldKeys - Array of previous sgo external keys.
 * @param {string[]} newKeys - Array of current sgo external keys.
 * @param {object} mongo - MongoDB connection or client instance.
 * @param {object} config - Configuration object for the aggregation process.
 * @param {object} nationRef - Reference object for the nation being updated.
 * @param {string} requestId - Unique identifier for the current request, used for logging or tracing.
 * @returns {Promise<void>} Resolves when all references have been updated.
 */
async function updateSgoNationReferences(oldKeys, newKeys, mongo, config, nationRef, requestId) {
	const remove = oldKeys.filter((k) => !newKeys.includes(k));
	for (const rk of remove) {
		const target = { resourceType: 'sgo', externalKey: { old: rk || null, new: null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, target, nationRef, requestId);
	}
	const add = newKeys.filter((k) => !oldKeys.includes(k));
	for (const ak of add) {
		const target = { resourceType: 'sgo', externalKey: { old: null, new: ak || null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, target, nationRef, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Updates references to team nations in aggregation documents by comparing old and new keys.
 *
 * For each key removed from the oldKeys array, updates the aggregation documents to remove the reference.
 * For each key added in the newKeys array, updates the aggregation documents to add the reference.
 *
 * @async
 * @param {string[]} oldKeys - Array of previous team external keys.
 * @param {string[]} newKeys - Array of current team external keys.
 * @param {object} mongo - MongoDB connection or client instance.
 * @param {object} config - Configuration object for the aggregation process.
 * @param {object} nationRef - Reference object for the nation being updated.
 * @param {string} requestId - Unique identifier for the current request, used for logging or tracing.
 * @returns {Promise<void>} Resolves when all references have been updated.
 */
async function updateTeamNationReferences(oldKeys, newKeys, mongo, config, nationRef, requestId) {
	const remove = oldKeys.filter((k) => !newKeys.includes(k));
	for (const rk of remove) {
		const target = { resourceType: 'team', externalKey: { old: rk || null, new: null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, target, nationRef, requestId);
	}
	const add = newKeys.filter((k) => !oldKeys.includes(k));
	for (const ak of add) {
		const target = { resourceType: 'team', externalKey: { old: null, new: ak || null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, target, nationRef, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
async function updateVenueNationReferences(oldKeys, newKeys, mongo, config, nationRef, requestId) {
	const remove = oldKeys.filter((k) => !newKeys.includes(k));
	for (const rk of remove) {
		const target = { resourceType: 'venue', externalKey: { old: rk || null, new: null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, target, nationRef, requestId);
	}
	const add = newKeys.filter((k) => !oldKeys.includes(k));
	for (const ak of add) {
		const target = { resourceType: 'venue', externalKey: { old: null, new: ak || null } };
		await updateResourceReferencesInAggregationDocs(mongo, config, target, nationRef, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processNation };
