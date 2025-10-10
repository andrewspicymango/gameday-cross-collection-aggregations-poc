const { debug } = require('../../log');

////////////////////////////////////////////////////////////////////////////////
async function updateResourceReferencesInCompetitionAggregations(
	mongo,
	config,
	oldCompetitionScopeAndId,
	newCompetitionScopeAndId,
	resourceType,
	resourceObjectId,
	resourceKey,
	requestId
) {
	const operations = [];
	const needsCompetitionUpdate = oldCompetitionScopeAndId.id !== newCompetitionScopeAndId.id || oldCompetitionScopeAndId.scope !== newCompetitionScopeAndId.scope;
	// if (!needsCompetitionUpdate) return;
	const { competitionAggregationTargetType } = require('./competitionAggregation');
	const targetResourceType = 'competition';
	const targetType = competitionAggregationTargetType;
	let aggregationToManage = {};
	if (resourceType === 'stage') aggregationToManage = { stages: resourceObjectId, stageKeys: resourceKey };
	if (resourceType === 'event') aggregationToManage = { events: resourceObjectId, eventKeys: resourceKey };
	if (resourceType === 'sgo') aggregationToManage = { sgos: resourceObjectId, sgoKeys: resourceKey };
	if (resourceType === 'team') aggregationToManage = { teams: resourceObjectId, teamKeys: resourceKey };
	if (resourceType === 'sportsPerson') aggregationToManage = { sportsPersons: resourceObjectId, sportsPersonKeys: resourceKey };
	if (resourceType === 'venue') aggregationToManage = { venues: resourceObjectId, venueKeys: resourceKey };
	if (resourceType === 'keyMoment') aggregationToManage = { keyMoments: resourceObjectId, keyMomentKeys: resourceKey };

	////////////////////////////////////////////////////////////////////////////
	// Remove from old competition
	if (oldCompetitionScopeAndId.id && oldCompetitionScopeAndId.scope) {
		const filter = { targetResourceType, _externalIdScope: oldCompetitionScopeAndId.scope, _externalId: oldCompetitionScopeAndId.id, targetType };
		const update = { $pull: aggregationToManage, $set: { lastUpdated: new Date() } };
		operations.push({ updateOne: { filter, update } });
	}
	///////////////////////////////////////////////////////////////////////////
	// Add to new competition
	if (newCompetitionScopeAndId.id && newCompetitionScopeAndId.scope) {
		const filter = { targetResourceType, _externalIdScope: newCompetitionScopeAndId.scope, _externalId: newCompetitionScopeAndId.id, targetType };
		const update = { $addToSet: aggregationToManage, $set: { lastUpdated: new Date() } };
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
module.exports = { updateResourceReferencesInCompetitionAggregations };
