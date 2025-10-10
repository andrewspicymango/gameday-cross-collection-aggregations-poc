const _ = require('lodash');
const { debug } = require('../log');

////////////////////////////////////////////////////////////////////////////////
/**
 * Update references to a resource across aggregation documents in a materialized-aggregation collection.
 *
 * This function validates inputs, determines whether a resource reference needs to be moved
 * from one aggregation root to another (based on id + scope differences), and prepares MongoDB
 * bulkWrite operations to:
 *   - $pull the reference fields from the old aggregation document (if old id/scope provided)
 *   - $addToSet the reference fields into the new aggregation document (if new id/scope provided)
 * Each write also sets a `lastUpdated` timestamp. The new aggregation write uses upsert: true.
 *
 * The mapping between resourceReferenceType and the fields updated in the aggregation documents is:
 *   - "competition"     => { competitions, competitionKeys }
 *   - "stage"           => { stages, stageKeys }
 *   - "event"           => { events, eventKeys }
 *   - "sgo"             => { sgos, sgoKeys }
 *   - "team"            => { teams, teamKeys }
 *   - "sportsPerson"    => { sportsPersons, sportsPersonKeys }
 *   - "venue"           => { venues, venueKeys }
 *   - "keyMoment"       => { keyMoments, keyMomentKeys }
 *
 * Notes:
 *   - If aggregationForOld and aggregationForNew have the same id and scope, the function is a no-op.
 *   - Bulk operations are only executed when there is at least one update to perform.
 *   - The function expects config.mongo.matAggCollectionName to be a string and uses mongo.db.collection(...) to run bulkWrite.
 *
 * @async
 * @param {Object} mongo - Mongo wrapper exposing a `db` property with a `collection(name)` function that supports bulkWrite.
 * @param {Object} config - Configuration object.
 * @param {Object} config.mongo - Mongo-specific configuration.
 * @param {string} config.mongo.matAggCollectionName - Name of the materialized aggregation collection to update.
 * @param {string} aggregationForResourceType - The resource type of the aggregation documents to update (e.g. "match", "fixture").
 * @param {Object} aggregationForOld - The current (old) aggregation root identifier.
 * @param {(string|number|null)} aggregationForOld.id - The external id of the old aggregation root (may be null/undefined to indicate absence).
 * @param {(string|null)} aggregationForOld.scope - The scope/namespace of the old aggregation root (may be null/undefined to indicate absence).
 * @param {Object} aggregationForNew - The target (new) aggregation root identifier.
 * @param {(string|number|null)} aggregationForNew.id - The external id of the new aggregation root (may be null/undefined to indicate absence).
 * @param {(string|null)} aggregationForNew.scope - The scope/namespace of the new aggregation root (may be null/undefined to indicate absence).
 * @param {string} aggregationForTargetType - The targetType value that aggregation documents must have to be considered.
 * @param {string} resourceReferenceType - The type of resource being referenced (must be one of the documented mapping keys above).
 * @param {*} resourceReferenceObjectId - The stored object id/value for the resource reference (e.g. ObjectId or primitive id).
 * @param {string} resourceReferenceKey - The key/string identifier associated with the resource reference (stored alongside the id).
 * @param {string} [requestId] - Optional request identifier for logging/debugging. Defaults to 'no-request-id-provided' if not provided.
 *
 * @throws {Error} If required parameters are missing or of the wrong type. Specific checks:
 *   - config.mongo.matAggCollectionName must be a string
 *   - aggregationForResourceType and aggregationForTargetType must be strings
 *   - aggregationForOld and aggregationForNew must be objects
 *   - resourceReferenceType must be a string
 *   - resourceReferenceObjectId must be provided
 *   - resourceReferenceKey must be a string
 *
 * @returns {Promise<void>} Resolves when any necessary bulkWrite operations have completed.
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
	if (aggregationForOld?.id != null && aggregationForOld?.scope != null) {
		filter._externalIdScope = aggregationForOld.scope;
		filter._externalId = aggregationForOld.id;
		const update = { $pull: referenceToManage, $set: { lastUpdated: new Date() } };
		operations.push({ updateOne: { filter, update } });
	}
	///////////////////////////////////////////////////////////////////////////
	// Add to new aggregation for root (if exists)
	if (aggregationForNew?.id != null && aggregationForNew?.scope != null) {
		filter._externalIdScope = aggregationForNew.scope;
		filter._externalId = aggregationForNew.id;
		const update = { $addToSet: referenceToManage, $set: { lastUpdated: new Date() } };
		operations.push({ updateOne: { filter, update, upsert: true } });
	}
	//////////////////////////////////////////////////////////////////////////////
	// Execute bulk operations if any
	if (operations.length > 0) {
		await mongo.db.collection(config.mongo.matAggCollectionName).bulkWrite(operations);
		debug(`Updated ${aggregationForResourceType} aggregation references for ${resourceReferenceType} ${resourceReferenceKey}`, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { updateResourceReferencesInAggregationDoc };
