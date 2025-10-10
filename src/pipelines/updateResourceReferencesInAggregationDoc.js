const _ = require('lodash');
const { debug } = require('../log');

////////////////////////////////////////////////////////////////////////////////
/**
 * Update resource reference arrays in a materialized aggregation collection when an aggregation reference
 * (the document identified by an external scope + id) moves from one external id/scope to another.
 *
 * The function:
 * - Validates inputs and required configuration.
 * - Determines which reference fields to manage based on resourceReferenceType (e.g. 'competition' -> { competitions, competitionKeys }).
 * - If the old aggregation root exists (has both scope and id) it issues a $pull update to remove the resource id/key from that aggregation document.
 * - If the new aggregation root exists (has both scope and id) it issues an $addToSet update (with upsert: true) to add the resource id/key to that aggregation document.
 * - Sets lastUpdated on any modified aggregation document.
 * - Executes the updates as a single bulkWrite operation when there are any operations to perform.
 * - No-op and returns early when the root scope/id did not change (no work required).
 *
 * Notes:
 * - For the "remove" operation a $pull is used; for the "add" operation $addToSet is used to avoid duplicates.
 * - The "add" operation uses upsert: true so a missing aggregation document for the new root will be created.
 * - The function logs a debug message after a successful bulkWrite.
 *
 * @async
 * @function updateResourceReferencesInAggregationDoc
 * @param {Object} mongo - MongoDB helper object exposing .db.collection(...).bulkWrite(...)
 * @param {Object} config - Configuration object. Must contain mongo.matAggCollectionName (string).
 * @param {string} aggregationForResourceType - Resource type of the aggregation root (used to match documents by resourceType).
 * @param {{scope: string, id: string}} aggregationForOld - Previous external id + scope identifying the aggregation root to remove the reference from.
 * @param {{scope: string, id: string}} aggregationForNew - New external id + scope identifying the aggregation root to add the reference to.
 * @param {string} aggregationForTargetType - Target type of the aggregation root (used to match documents by targetType).
 * @param {string} resourceReferenceType - Type of the referenced resource (one of the supported values listed above).
 * @param {string|Object} resourceReferenceObjectId - The resource object id to add/remove from the aggregation arrays (may be ObjectId or string).
 * @param {string} resourceReferenceKey - The external key for the resource to add/remove from the corresponding "...Keys" array.
 * @param {string} [requestId] - Optional request id used for logging; defaults to 'no-request-id-provided' when not supplied.
 *
 * @throws {Error} When required arguments are missing or invalid (including missing config.mongo.matAggCollectionName or invalid parameter types).
 *
 * @returns {Promise<void>} Resolves when the bulk operation (if any) has completed. If no update is required the promise resolves immediately.
 */
async function updateResourceReferencesInAggregationDoc(
	mongo,
	config,
	aggregationForResourceType,
	aggregationForOld,
	aggregationForNew,
	aggregationForTargetType,
	resourceReferenceType,
	resourceReferenceObjectId,
	resourceReferenceKey,
	requestId
) {
	//////////////////////////////////////////////////////////////////////////////
	// Validate inputs
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!aggregationForResourceType || !_.isString(aggregationForResourceType))
		throw new Error('Invalid parameters: aggregationRootResourceType is required and must be a string');
	if (!aggregationForTargetType || !_.isString(aggregationForTargetType)) throw new Error('Invalid parameters: aggregationRootTargetType is required and must be a string');
	if (!aggregationForOld || !_.isObject(aggregationForOld)) throw new Error('Invalid parameters: oldAggregationRootScopeAndId is required and must be an object');
	if (!aggregationForNew || !_.isObject(aggregationForNew)) throw new Error('Invalid parameters: newAggregationRootScopeAndId is required and must be an object');
	if (!resourceReferenceType || !_.isString(resourceReferenceType)) throw new Error('Invalid parameters: resourceReferenceType is required and must be a string');
	if (!resourceReferenceObjectId) throw new Error('Invalid parameters: resourceReferenceObjectId is required');
	if (!resourceReferenceKey || !_.isString(resourceReferenceKey)) throw new Error('Invalid parameters: resourceReferenceKey is required and must be a string');
	if (!requestId || !_.isString(requestId)) requestId = 'no-request-id-provided';

	//////////////////////////////////////////////////////////////////////////////
	// Prepare update operations
	const operations = [];
	const needsUpdate = aggregationForOld.id !== aggregationForNew.id || aggregationForOld.scope !== aggregationForNew.scope;
	if (!needsUpdate) return;
	let referenceToManage = {};
	if (resourceReferenceType === 'competition') referenceToManage = { competitions: resourceReferenceObjectId, competitionKeys: resourceReferenceKey };
	else if (resourceReferenceType === 'stage') referenceToManage = { stages: resourceReferenceObjectId, stageKeys: resourceReferenceKey };
	else if (resourceReferenceType === 'event') referenceToManage = { events: resourceReferenceObjectId, eventKeys: resourceReferenceKey };
	else if (resourceReferenceType === 'sgo') referenceToManage = { sgos: resourceReferenceObjectId, sgoKeys: resourceReferenceKey };
	else if (resourceReferenceType === 'team') referenceToManage = { teams: resourceReferenceObjectId, teamKeys: resourceReferenceKey };
	else if (resourceReferenceType === 'sportsPerson') referenceToManage = { sportsPersons: resourceReferenceObjectId, sportsPersonKeys: resourceReferenceKey };
	else if (resourceReferenceType === 'venue') referenceToManage = { venues: resourceReferenceObjectId, venueKeys: resourceReferenceKey };
	else if (resourceReferenceType === 'keyMoment') referenceToManage = { keyMoments: resourceReferenceObjectId, keyMomentKeys: resourceReferenceKey };
	const filter = { resourceType: aggregationForResourceType, targetType: aggregationForTargetType };

	////////////////////////////////////////////////////////////////////////////
	// Remove from old aggregation for root (if exists)
	if (aggregationForOld.id && aggregationForOld.scope) {
		filter._externalIdScope = aggregationForOld.scope;
		filter._externalId = aggregationForOld.id;
		const update = { $pull: referenceToManage, $set: { lastUpdated: new Date() } };
		operations.push({ updateOne: { filter, update } });
	}
	///////////////////////////////////////////////////////////////////////////
	// Add to new aggregation for root (if exists)
	if (aggregationForNew.id && aggregationForNew.scope) {
		filter._externalIdScope = aggregationForNew.scope;
		filter._externalId = aggregationForNew.id;
		const update = { $addToSet: referenceToManage, $set: { lastUpdated: new Date() } };
		operations.push({ updateOne: { filter, update, upsert: true } });
	}
	//////////////////////////////////////////////////////////////////////////////
	// Execute bulk operations if any
	if (operations.length > 0) {
		await mongo.db.collection(config.mongo.matAggCollectionName).bulkWrite(operations);
		debug(`Updated competition references for stage ${stageKey}`, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { updateResourceReferencesInAggregationDoc };
