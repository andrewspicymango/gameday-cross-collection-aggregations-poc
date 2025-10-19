const _ = require('lodash');
const { debug, warn } = require('../log');

////////////////////////////////////////////////////////////////////////////////
// What are doing?
// When a gameday resource's aggregation document set of outbound references change, we need to go the referencing aggregation documents
// and update their references to include the new reference and remove the old reference (if any).
//  - e.g. a competition's aggregation document has references to sgos and stages. If any of these change, we need to update
//         competitionIds and competitionKeys in the affected sgo and stage aggregation documents.
//  - e.g. a stage's aggregation document has references to competitions and events. If any of these change, we need to update
//         stageIds and stageKeys in the affected competition and event aggregation documents.
// To manage this updating of references we have:
// - a `managedResource` (e.g. the competition) that is being updated. This will include a resourceType (e.g. `competition`)
// - a `managedResourceReference` which is the reference to the managedResource that is being added/removed across other aggregation documents
// - a set of `oldOutboundReferences` which are the aggregation documents that referenced the managedResource before the update
// - a set of `newOutboundReferences` which are the aggregation documents that reference the managedResource after the update
//
// `outboundReferences` are structured as:
// {
//    sgo: { [externalKey]: <ObjectId>, ... },
//    stage: { [externalKey]: <ObjectId>, ... },
//    ... etc,
// }
function buildOperationsForReferenceChange(oldAggregationDoc, newAggregationDoc, operations = null) {
	if (!_.isObject(newAggregationDoc)) throw new Error('Invalid parameters: newAggregationDoc must be an object');
	if (!_.isString(newAggregationDoc?.resourceType)) throw new Error('Invalid parameters: newAggregationDoc.resourceType must be a string');
	if (!_.isString(newAggregationDoc?.externalKey)) throw new Error('Invalid parameters: newAggregationDoc.externalKey must be an object');
	if (!_.isObject(newAggregationDoc?.gamedayId)) throw new Error('Invalid parameters: newAggregationDoc.gamedayId must be an object');
	if (!_.isObject(oldAggregationDoc)) oldAggregationDoc = {};
	if (!Array.isArray(operations)) operations = [];
	for (const rt of ['sgo', 'competition', 'stage', 'event', 'team', 'club', 'nation', 'sportsPerson', 'venue', 'keyMoment', 'staff', 'ranking']) {
		const oldReferences = oldAggregationDoc[`${rt}Keys`] || {};
		const newReferences = newAggregationDoc[`${rt}Keys`] || {};
		const oldKeys = Object.keys(oldReferences);
		const newKeys = Object.keys(newReferences);
		if (oldKeys.length === 0 && newKeys.length === 0) continue; // nothing to do
		const referenceToManage = {
			[`${newAggregationDoc.resourceType}s`]: newAggregationDoc.gamedayId,
			[`${newAggregationDoc.resourceType}Keys`]: newAggregationDoc.externalKey,
		};
		const keysToRemoveResourceFrom = oldKeys.filter((oldKey) => !newKeys.includes(oldKey));
		const keysToAddResourceTo = newKeys.filter((newKey) => !oldKeys.includes(newKey));
		////////////////////////////////////////////////////////////////////////////
		// Build operations for removing the resource from aggregation documents
		for (const oldKey of keysToRemoveResourceFrom) {
			const filter = { resourceType, externalKey: oldKey };
			const update = { $pull: referenceToManage, $set: { lastUpdated: new Date() } };
			operations.push({ updateOne: { filter, update } });
		}
		///////////////////////////////////////////////////////////////////////////
		// Build operations for adding the resource to aggregation documents
		for (const newKey of keysToAddResourceTo) {
			const filter = { resourceType: rt, externalKey: newKey };
			const update = {
				$addToSet: {
					[`${newAggregationDoc.resourceType}s`]: newAggregationDoc.gamedayId,
				},
				$set: {
					[`${newAggregationDoc.resourceType}Keys.${newAggregationDoc.externalKey}`]: newAggregationDoc.gamedayId,
					lastUpdated: new Date(),
				},
				$setOnInsert: { gamedayId: newReferences[newKey] },
			};
			operations.push({ updateOne: { filter, update, upsert: true } });
		}
	}
	return operations;
}

////////////////////////////////////////////////////////////////////////////////
async function executeOperationsForReferenceChange(mongo, config, operations, requestId) {
	if (!operations || operations.length === 0) return;
	try {
		const result = await mongo.db.collection(config.mongo.matAggCollectionName).bulkWrite(operations);
		debug(`Updated aggregation references, result: ${JSON.stringify(result)}`, requestId);
	} catch (error) {
		warn(`Error executing bulk write: ${error.message}`, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { buildOperationsForReferenceChange, executeOperationsForReferenceChange };
