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
const { keySeparator } = require('../constants');
const { updateResourceReferencesInAggregationDocs } = require('../updateResourceReferencesInAggregationDocs');
const { pipeline } = require('./eventAggregationPipeline');
const { queryForEventAggregationDoc } = require('./eventAggregationPipeline');

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
	// A event has outbound references to:
	// - stage
	// - event
	// - teams (via participants)
	// - sportsPersons (via participants)
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(eventAggregationDocQuery);
	const oldStageExternalKey = oldAggregationDoc?.stageKeys[0];
	const oldVenueExternalKey = oldAggregationDoc?.venueKeys[0];
	// TODO: teams and sportsPersons
	//////////////////////////////////////////////////////////////////////////////
	// Build the event aggregation view
	await runPipeline(mongo, 'events', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the event aggregation and calculate new competition and stage keys
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(eventAggregationDocQuery);
	//////////////////////////////////////////////////////////////////////////////
	// FIX OUTBOUND REFERENCES
	const eventObjectId = newAggregationDoc?.gamedayId;
	const eventKey = `${eventId}${keySeparator}${eventIdScope}`;
	const eventResourceReference = { resourceType: 'event', externalKey: eventKey, objectId: eventObjectId };
	//////////////////////////////////////////////////////////////////////////////
	// _externalStageId
	const newStageExternalKey = newAggregationDoc?.stageKeys[0];
	const stageAggregationDoc = { resourceType: 'stage', externalKey: { old: oldStageExternalKey || null, new: newStageExternalKey || null } };
	await updateResourceReferencesInAggregationDocs(mongo, config, stageAggregationDoc, eventResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// _externalVenueId
	const newVenueExternalKey = newAggregationDoc?.venueKeys[0];
	const venueAggregationDocs = { resourceType: 'venue', externalKey: { old: oldVenueExternalKey || null, new: newVenueExternalKey || null } };
	await updateResourceReferencesInAggregationDocs(mongo, config, venueAggregationDocs, eventResourceReference, requestId);
	//////////////////////////////////////////////////////////////////////////////
	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processEvent };
