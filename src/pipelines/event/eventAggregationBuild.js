////////////////////////////////////////////////////////////////////////////////
// When a stage is created or updated, we need to build its aggregated view we need to:
// - build the full materialised view for the stage
// - we also need to update any competitions that reference this stage
//   (as they have a list of stage ids/keys in their materialised view, and this stage
//   may be new to that list or moved from a previous competition to a new one)
//
// The process is:
// 1. Retrieve the previous version of the stage aggregation (if it exists) to determine the competition
//    it was previously associated with (if any)
// 2. Build the stage aggregation view
// 3. If the stage was previously associated with a competition, rebuild that competition's
//    aggregation view to ensure it is up to date
// 4. If the stage is now associated with a different competition, rebuild that competition's
//    aggregation view to ensure it is up to date
////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { splitKey } = require('../splitKey');
const { keySeparator } = require('../constants');
const { pipeline } = require('./eventAggregationPipeline');
const { queryForEventAggregationDoc } = require('./eventAggregationPipeline');
const { updateResourceReferencesInAggregationDoc } = require('../updateResourceReferencesInAggregationDoc');
const { stageAggregationTargetType } = require('../stage/stageAggregationPipeline');

////////////////////////////////////////////////////////////////////////////////
async function processEvent(config, mongo, eventIdScope, eventId, requestId) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!eventId || !eventIdScope) throw new Error('Invalid parameters: eventId and eventIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`eventIdScope=${eventIdScope}, eventId=${eventId}`, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Check if event exists before running expensive pipeline
	const eventExists = await mongo.db.collection('events').countDocuments({ _externalId: eventId, _externalIdScope: eventIdScope }, { limit: 1 });
	//////////////////////////////////////////////////////////////////////////////
	if (eventExists === 0) {
		debug(`Event not found: ${eventId}@${eventIdScope}`, requestId);
		return 404;
	}

	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, eventIdScope, eventId);
	const eventAggregationDocQuery = queryForEventAggregationDoc(eventId, eventIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the previous version of the event aggregation (if it exists) and calculate old competition and stage keys
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(eventAggregationDocQuery);
	const oldStageIdAndScope = splitKey(oldAggregationDoc?.stageKeys[0]);
	//////////////////////////////////////////////////////////////////////////////
	// Build the event aggregation view
	await runPipeline(mongo, 'events', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the event aggregation and calculate new competition and stage keys
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(eventAggregationDocQuery);
	const newStageIdAndScope = splitKey(newAggregationDoc?.stageKeys[0]);
	const eventObjectId = newAggregationDoc?.gamedayId;
	const eventKey = `${eventId}${keySeparator}${eventIdScope}`;

	//////////////////////////////////////////////////////////////////////////////
	// FIX UPWARDS REFERENCES
	//////////////////////////////////////////////////////////////////////////////
	// The event may have moved stages if either the stage id or scope has changed
	// So we may need to update stage aggregations
	await updateResourceReferencesInAggregationDoc(
		mongo,
		config,
		'stage',
		oldStageIdAndScope,
		newStageIdAndScope,
		stageAggregationTargetType,
		'event',
		eventObjectId,
		eventKey,
		requestId
	);

	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
async function updateStageEventReferences(mongo, config, { oldStageIdAndScope, newStageIdAndScope, eventObjectId, eventKey, eventData, requestId }) {
	const operations = [];
	//////////////////////////////////////////////////////////////////////////////
	// Remove from old stage (if exists)
	if (oldStageIdAndScope.id && oldStageIdAndScope.scope) {
		operations.push({
			updateOne: {
				filter: { resourceType: 'stage', _externalId: oldStageIdAndScope.id, _externalIdScope: oldStageIdAndScope.scope, targetType: stageAggregationTargetType },
				update: { $pull: { events: { _id: eventObjectId }, eventKeys: eventKey }, $set: { lastUpdated: new Date() } },
			},
		});
	}
	//////////////////////////////////////////////////////////////////////////////
	// Add to new stage (if different)
	if (newStage.id && newStage.scope) {
		operations.push({
			updateOne: {
				filter: { resourceType: 'stage', _externalId: newStageIdAndScope.id, _externalIdScope: newStageIdAndScope.scope, targetType: stageAggregationTargetType },
				update: { $addToSet: { events: { _id: eventObjectId }, eventKeys: eventKey }, $set: { lastUpdated: new Date() } },
				upsert: true,
			},
		});
	}
	//////////////////////////////////////////////////////////////////////////////
	// Execute bulk operations if any
	if (operations.length > 0) {
		await mongo.db.collection(config.mongo.matAggCollectionName).bulkWrite(operations);
		debug(`Updated stage references for event ${eventKey}`, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processEvent };
