const { debug } = require('../../log');

////////////////////////////////////////////////////////////////////////////////
async function updateResourceReferencesInStageAggregations(mongo, config, oldStageScopeAndId, newStageScopeAndId, resourceType, resourceObjectId, resourceKey, requestId) {
	const operations = [];
	const needsStageUpdate = oldStageScopeAndId.id !== newStageScopeAndId.id || oldStageScopeAndId.scope !== newStageScopeAndId.scope;
	// if (!needsStageUpdate) return;
	const { stageAggregationTargetType } = require('./stageAggregation');
	const targetResourceType = 'stage';
	const targetType = stageAggregationTargetType;
	let aggregationToManage = {};
	if (resourceType === 'competition') aggregationToManage = { competitions: resourceObjectId, competitionKeys: resourceKey };
	if (resourceType === 'event') aggregationToManage = { events: resourceObjectId, eventKeys: resourceKey };
	if (resourceType === 'sgo') aggregationToManage = { sgos: resourceObjectId, sgoKeys: resourceKey };
	if (resourceType === 'team') aggregationToManage = { teams: resourceObjectId, teamKeys: resourceKey };
	if (resourceType === 'sportsPerson') aggregationToManage = { sportsPersons: resourceObjectId, sportsPersonKeys: resourceKey };
	if (resourceType === 'venue') aggregationToManage = { venues: resourceObjectId, venueKeys: resourceKey };
	if (resourceType === 'keyMoment') aggregationToManage = { keyMoments: resourceObjectId, keyMomentKeys: resourceKey };

	////////////////////////////////////////////////////////////////////////////
	// Remove from old stage
	if (oldStageScopeAndId.id && oldStageScopeAndId.scope) {
		const filter = { targetResourceType, _externalIdScope: oldStageScopeAndId.scope, _externalId: oldStageScopeAndId.id, targetType };
		const update = { $pull: aggregationToManage, $set: { lastUpdated: new Date() } };
		operations.push({ updateOne: { filter, update } });
	}
	///////////////////////////////////////////////////////////////////////////
	// Add to new stage
	if (newStageScopeAndId.id && newStageScopeAndId.scope) {
		const filter = { targetResourceType, _externalIdScope: newStageScopeAndId.scope, _externalId: newStageScopeAndId.id, targetType };
		const update = { $addToSet: aggregationToManage, $set: { lastUpdated: new Date() } };
		operations.push({ updateOne: { filter, update, upsert: true } });
	}
	//////////////////////////////////////////////////////////////////////////////
	// Execute bulk operations if any
	if (operations.length > 0) {
		await mongo.db.collection(config.mongo.matAggCollectionName).bulkWrite(operations);
		debug(`Updated stage references for stage ${stageKey}`, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { updateResourceReferencesInStageAggregations };
