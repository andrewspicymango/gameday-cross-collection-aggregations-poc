const _ = require('lodash');
const { debug, warn } = require('../log');
const { keySeparator } = require('./constants');

////////////////////////////////////////////////////////////////////////////////
const collectionNamesAndKeys = {
	competition: { name: 'competitions', 0: '_externalId', 1: '_externalIdScope' },
	stage: { name: 'stages', 0: '_externalId', 1: '_externalIdScope' },
	event: { name: 'events', 0: '_externalId', 1: '_externalIdScope' },
	sgo: { name: 'sgos', 0: '_externalId', 1: '_externalIdScope' },
	team: { name: 'teams', 0: '_externalId', 1: '_externalIdScope' },
	club: { name: 'clubs', 0: '_externalId', 1: '_externalIdScope' },
	sportsPerson: { name: 'sportsPersons', 0: '_externalId', 1: '_externalIdScope' },
	venue: { name: 'venues', 0: '_externalId', 1: '_externalIdScope' },
	keyMoment: { name: 'keyMoments', 0: 'dateTime', 1: '_externalEventId', 2: '_externalEventIdScope', 3: 'type', 4: 'subType' },
	staff: { name: 'staff', 0: '_externalSportsPersonId', 1: '_externalSportsPersonIdScope' },
};

////////////////////////////////////////////////////////////////////////////////
function buildOperationsForUpdateResourceReferencesInAggregationDocs(aggregationDoc, resourceReference, operations) {
	//////////////////////////////////////////////////////////////////////////////
	/** Predicate for a Mongo facet: returns true when the provided prop is null or a string; */
	function propStringOrNull(prop) {
		return prop === null || _.isString(prop);
	}
	//////////////////////////////////////////////////////////////////////////////
	// Validate inputs
	if (!_.isObject(aggregationDoc)) throw new Error('Invalid parameters: aggregationDoc is required and must be an object');
	if (!_.isString(aggregationDoc.resourceType)) throw new Error('Invalid parameters: aggregationDoc.resourceType is required and must be a string');
	if (!_.isObject(aggregationDoc.externalKey)) throw new Error('Invalid parameters: aggregationDoc.externalKey is required and must be an object');
	if (!propStringOrNull(aggregationDoc.externalKey?.old)) throw new Error('Invalid parameters: aggregationDoc.externalKey.old must be a string or null');
	if (!propStringOrNull(aggregationDoc.externalKey?.new)) throw new Error('Invalid parameters: aggregationDoc.externalKey.new must be a string or null');
	if (!_.isObject(resourceReference)) throw new Error('Invalid parameters: resourceReference is required and must be an object');
	if (!_.isString(resourceReference.resourceType)) throw new Error('Invalid parameters: resourceReference.resourceType is required and must be a string');
	if (!_.isString(resourceReference.externalKey)) throw new Error('Invalid parameters: resourceReference.externalKey is required and must be a string');
	if (!resourceReference.objectId) throw new Error('Invalid parameters: resourceReference.objectId is required');

	//////////////////////////////////////////////////////////////////////////////
	// Prepare update operations
	if (!Array.isArray(operations)) operations = [];
	const needsUpdate = aggregationDoc.externalKey.old !== aggregationDoc.externalKey.new;
	if (!needsUpdate) return;
	let referenceToManage = {};
	const rtLowerCase = resourceReference.resourceType.toLowerCase();
	if (rtLowerCase === 'competition') referenceToManage = { competitions: resourceReference.objectId, competitionKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'stage') referenceToManage = { stages: resourceReference.objectId, stageKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'event') referenceToManage = { events: resourceReference.objectId, eventKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'sgo') referenceToManage = { sgos: resourceReference.objectId, sgoKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'team') referenceToManage = { teams: resourceReference.objectId, teamKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'club') referenceToManage = { clubs: resourceReference.objectId, clubKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'venue') referenceToManage = { venues: resourceReference.objectId, venueKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'staff') referenceToManage = { staff: resourceReference.objectId, staffKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'keymoment') referenceToManage = { keyMoments: resourceReference.objectId, keyMomentKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'sportsperson') referenceToManage = { sportsPersons: resourceReference.objectId, sportsPersonKeys: resourceReference.externalKey };
	const baseFilter = { resourceType: aggregationDoc.resourceType };

	////////////////////////////////////////////////////////////////////////////
	// Remove from old aggregation (if exists)
	if (aggregationDoc.externalKey.old != null) {
		const filterOld = { ...baseFilter, externalKey: aggregationDoc.externalKey.old };
		const update = { $pull: referenceToManage, $set: { lastUpdated: new Date() } };
		operations.push({ updateOne: { filter: filterOld, update } });
	}

	///////////////////////////////////////////////////////////////////////////
	// Add to new aggregation (if exists)
	if (aggregationDoc.externalKey.new != null) {
		const filterNew = { ...baseFilter, externalKey: aggregationDoc.externalKey.new };
		const update = { $addToSet: referenceToManage, $set: { lastUpdated: new Date() } };
		if (aggregationDoc.gamedayId) update.$setOnInsert = { gamedayId: aggregationDoc.gamedayId };
		operations.push({ updateOne: { filter: filterNew, update, upsert: true } });
	}
}

////////////////////////////////////////////////////////////////////////////////
async function executeOperationsForUpdateResourceReferencesInAggregationDocs(mongo, config, operations, requestId) {
	if (!operations || operations.length === 0) return;
	try {
		const result = await mongo.db.collection(config.mongo.matAggCollectionName).bulkWrite(operations);
		debug(`Updated aggregation references, result: ${JSON.stringify(result)}`, requestId);
	} catch (error) {
		warn(`Error executing bulk write: ${error.message}`, requestId);
	}
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Update resource references inside an aggregation document facet.
 *
 * Validates inputs and constructs Mongo bulk operations to:
 *  - remove the resource reference from the old aggregation ($pull) if externalKey.old exists
 *  - add the resource reference to the new aggregation ($addToSet, upsert) if externalKey.new exists
 *  - set lastUpdated to the current date on affected aggregation docs
 *
 * @async
 * @function
 * @param {object} mongo - Mongo client/handle exposing db.collection(...).bulkWrite(...)
 * @param {object} config - Config object containing mongo.matAggCollectionName
 * @param {object} aggregationDoc - Aggregation descriptor with { resourceType: string, externalKey: { old: string|null, new: string|null } }
 * @param {object} resourceReference - Reference to manage: { resourceType: string, externalKey: string, objectId: any }
 * @param {string} [requestId] - Optional request identifier for logging
 * @throws {Error} If required parameters are missing or have invalid types
 * @returns {Promise<void>} Resolves after performing bulkWrite (or immediately if no change is needed)
 */
async function updateResourceReferencesInAggregationDocs(mongo, config, aggregationDoc, resourceReference, requestId) {
	//////////////////////////////////////////////////////////////////////////////
	/** Predicate for a Mongo facet: returns true when the provided prop is null or a string; */
	function propStringOrNull(prop) {
		return prop === null || _.isString(prop);
	}

	//////////////////////////////////////////////////////////////////////////////
	// Validate inputs
	if (!_.isObject(aggregationDoc)) throw new Error('Invalid parameters: aggregationDoc is required and must be an object');
	if (!_.isString(aggregationDoc.resourceType)) throw new Error('Invalid parameters: aggregationDoc.resourceType is required and must be a string');
	if (!_.isObject(aggregationDoc.externalKey)) throw new Error('Invalid parameters: aggregationDoc.externalKey is required and must be an object');
	if (!propStringOrNull(aggregationDoc.externalKey?.old)) throw new Error('Invalid parameters: aggregationDoc.externalKey.old must be a string or null');
	if (!propStringOrNull(aggregationDoc.externalKey?.new)) throw new Error('Invalid parameters: aggregationDoc.externalKey.new must be a string or null');
	if (!_.isObject(resourceReference)) throw new Error('Invalid parameters: resourceReference is required and must be an object');
	if (!_.isString(resourceReference.resourceType)) throw new Error('Invalid parameters: resourceReference.resourceType is required and must be a string');
	if (!_.isString(resourceReference.externalKey)) throw new Error('Invalid parameters: resourceReference.externalKey is required and must be a string');
	if (!resourceReference.objectId) throw new Error('Invalid parameters: resourceReference.objectId is required');
	if (!requestId || !_.isString(requestId)) requestId = 'no-request-id-provided';

	//////////////////////////////////////////////////////////////////////////////
	// Prepare update operations
	const operations = [];
	const needsUpdate = aggregationDoc.externalKey.old !== aggregationDoc.externalKey.new;
	if (!needsUpdate) return;
	let referenceToManage = {};
	const rtLowerCase = resourceReference.resourceType.toLowerCase();
	if (rtLowerCase === 'competition') referenceToManage = { competitions: resourceReference.objectId, competitionKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'stage') referenceToManage = { stages: resourceReference.objectId, stageKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'event') referenceToManage = { events: resourceReference.objectId, eventKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'sgo') referenceToManage = { sgos: resourceReference.objectId, sgoKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'team') referenceToManage = { teams: resourceReference.objectId, teamKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'club') referenceToManage = { clubs: resourceReference.objectId, clubKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'venue') referenceToManage = { venues: resourceReference.objectId, venueKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'staff') referenceToManage = { staff: resourceReference.objectId, staffKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'keymoment') referenceToManage = { keyMoments: resourceReference.objectId, keyMomentKeys: resourceReference.externalKey };
	else if (rtLowerCase === 'sportsperson') referenceToManage = { sportsPersons: resourceReference.objectId, sportsPersonKeys: resourceReference.externalKey };
	const baseFilter = { resourceType: aggregationDoc.resourceType };

	////////////////////////////////////////////////////////////////////////////
	// Remove from old aggregation (if exists)
	if (aggregationDoc.externalKey.old != null) {
		const filterOld = { ...baseFilter, externalKey: aggregationDoc.externalKey.old };
		const update = { $pull: referenceToManage, $set: { lastUpdated: new Date() } };
		operations.push({ updateOne: { filter: filterOld, update } });
	}
	///////////////////////////////////////////////////////////////////////////
	// Add to new aggregation (if exists)
	let filterNew = null;
	if (aggregationDoc.externalKey.new != null) {
		if (aggregationDoc.gamedayId) {
			filterNew = { ...baseFilter, externalKey: aggregationDoc.externalKey.new, gamedayId: aggregationDoc.gamedayId };
		} else {
			filterNew = { ...baseFilter, externalKey: aggregationDoc.externalKey.new };
		}
		const update = { $addToSet: referenceToManage, $set: { lastUpdated: new Date() } };
		operations.push({ updateOne: { filter: filterNew, update, upsert: true } });
	}

	//////////////////////////////////////////////////////////////////////////////
	// Execute bulk operations if any
	let upsert = false;
	if (operations.length > 0) {
		const result = await mongo.db.collection(config.mongo.matAggCollectionName).bulkWrite(operations);
		upsert = result.upsertedCount || result.modifiedCount > 0;
		debug(`Updated ${aggregationDoc.resourceType} aggregation references for ${resourceReference.resourceType} ${resourceReference.externalKey}`, requestId);
	}
	//////////////////////////////////////////////////////////////////////////////
	// Get the aggregation doc and see if it has gamedayId set
	if (upsert && filterNew && aggregationDoc?.externalKey?.new && _.has(collectionNamesAndKeys, aggregationDoc.resourceType)) {
		const aggDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(filterNew);
		if (!aggDoc) throw new Error(`Failed to find aggregation doc after upsert: ${JSON.stringify(filterNew)}`);
		if (aggDoc?.gamedayId) return;
		const keyParts = aggregationDoc.externalKey.new.split(keySeparator);
		const collectionNameAndKeys = collectionNamesAndKeys[aggregationDoc.resourceType];
		const name = collectionNameAndKeys.name;
		const keyField1 = collectionNameAndKeys[0] || null;
		const keyField2 = collectionNameAndKeys[1] || null;
		const keyField3 = collectionNameAndKeys[2] || null;
		const keyField4 = collectionNameAndKeys[3] || null;
		const keyField5 = collectionNameAndKeys[4] || null;
		const query = {};
		if (keyField1 && keyParts[0]) query[keyField1] = keyParts[0];
		if (keyField2 && keyParts[1]) query[keyField2] = keyParts[1];
		if (keyField3 && keyParts[2]) query[keyField3] = keyParts[2];
		if (keyField4 && keyParts[3]) query[keyField4] = keyParts[3];
		if (keyField5 && keyParts[4]) query[keyField5] = keyParts[4];
		if (Object.keys(query).length === 0)
			throw new Error(`Cannot build query for ${aggregationDoc.resourceType} aggregation doc with externalKey ${aggregationDoc.externalKey.new}`);
		const aggDocResource = await mongo.db.collection(name).findOne(query, { projection: { _id: 1 } });
		if (aggDocResource?._id) {
			await mongo.db.collection(config.mongo.matAggCollectionName).updateOne(filterNew, { $set: { gamedayId: aggDocResource._id, ...query } });
			debug(`Fixed gamedayId for ${aggregationDoc.resourceType} aggregation ${aggregationDoc.externalKey.new}`, requestId);
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// Determine which keys to remove the resource from and which to add it to
function buildOperationsForReferenceChange(sourceAggregationDocReference, keyType, oldKeyIdMapping, newKeyIdMapping, operations) {
	//////////////////////////////////////////////////////////////////////////////
	function getForeignReferenceToManage(sourceAggregationDocReference) {
		switch (sourceAggregationDocReference.resourceType.toLowerCase()) {
			case 'competition':
				return { competitions: sourceAggregationDocReference.objectId, competitionKeys: sourceAggregationDocReference.externalKey };
			case 'stage':
				return { stages: sourceAggregationDocReference.objectId, stageKeys: sourceAggregationDocReference.externalKey };
			case 'event':
				return { events: sourceAggregationDocReference.objectId, eventKeys: sourceAggregationDocReference.externalKey };
			case 'sgo':
				return { sgos: sourceAggregationDocReference.objectId, sgoKeys: sourceAggregationDocReference.externalKey };
			case 'team':
				return { teams: sourceAggregationDocReference.objectId, teamKeys: sourceAggregationDocReference.externalKey };
			case 'club':
				return { clubs: sourceAggregationDocReference.objectId, clubKeys: sourceAggregationDocReference.externalKey };
			case 'venue':
				return { venues: sourceAggregationDocReference.objectId, venueKeys: sourceAggregationDocReference.externalKey };
			case 'staff':
				return { staff: sourceAggregationDocReference.objectId, staffKeys: sourceAggregationDocReference.externalKey };
			case 'keymoment':
				return { keyMoments: sourceAggregationDocReference.objectId, keyMomentKeys: sourceAggregationDocReference.externalKey };
			case 'sportsperson':
				return { sportsPersons: sourceAggregationDocReference.objectId, sportsPersonKeys: sourceAggregationDocReference.externalKey };
			default:
				throw new Error(`Unsupported resourceType: ${sourceAggregationDocReference.resourceType}`);
		}
	}

	//////////////////////////////////////////////////////////////////////////////
	// Validate input parameters
	if (!Array.isArray(operations)) throw new Error('Invalid parameters: operations must be an array');
	if (!_.isObject(sourceAggregationDocReference)) throw new Error('Invalid parameters: resourceReference must be an object with resourceType and externalKey');
	if (!_.isString(sourceAggregationDocReference.resourceType)) throw new Error('Invalid parameters: resourceReference must be an object with resourceType and externalKey');
	if (!_.isObject(sourceAggregationDocReference.externalKey)) throw new Error('Invalid parameters: resourceReference must be an object with resourceType and externalKey');
	if (!_.isObject(oldKeyIdMapping)) throw new Error('Invalid parameters: oldKeyIdMapping must be an object mapping externalKey to gamedayId');
	if (!_.isObject(newKeyIdMapping)) throw new Error('Invalid parameters: newKeyIdMapping must be an object mapping externalKey to gamedayId');
	if (!_.isString(keyType)) throw new Error('Invalid parameters: key type must be a string');

	//////////////////////////////////////////////////////////////////////////////
	// Convert objects to arrays of keys for processing
	const oldKeys = Object.keys(oldKeyIdMapping);
	const newKeys = Object.keys(newKeyIdMapping);

	//////////////////////////////////////////////////////////////////////////////
	// Determine which keys to remove the resource from and which to add it to
	const keysToRemoveResourceFrom = oldKeys.filter((oldKey) => !newKeys.includes(oldKey));
	const keysToAddResourceTo = newKeys.filter((newKey) => !oldKeys.includes(newKey));

	//////////////////////////////////////////////////////////////////////////////
	// What does the reference look like in the foreign aggregation document?
	const referenceToManage = getForeignReferenceToManage(sourceAggregationDocReference);

	//////////////////////////////////////////////////////////////////////////////
	// Build operations for removing the resource from aggregation documents
	for (const oldKey of keysToRemoveResourceFrom) {
		const filter = { resourceType: keyType, externalKey: oldKey };
		const update = { $pull: referenceToManage, $set: { lastUpdated: new Date() } };
		operations.push({ updateOne: { filter, update } });
	}

	//////////////////////////////////////////////////////////////////////////////
	// Build operations for adding the resource to aggregation documents
	for (const newKey of keysToAddResourceTo) {
		const filter = { resourceType: keyType, externalKey: newKey };
		const update = { $addToSet: referenceToManage, $set: { lastUpdated: new Date() } };
		operations.push({ updateOne: { filter, update } });
	}
}

////////////////////////////////////////////////////////////////////////////////
module.exports = {
	updateResourceReferencesInAggregationDocs,
	buildOperationsForUpdateResourceReferencesInAggregationDocs,
	executeOperationsForUpdateResourceReferencesInAggregationDocs,
	buildOperationsForReferenceChange,
};
