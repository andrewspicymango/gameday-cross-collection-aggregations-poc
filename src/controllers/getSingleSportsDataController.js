const _ = require(`lodash`);
const uuid = require('uuid');
const { send200, send400, send404, send500 } = require('../utils/httpResponseUtils');
const { debug, info, warn } = require('../log.js');
const config = require('../config.js');

const { writeFileSyncWithDirs } = require(`../utils/fileUtils.js`);

// curl localhost:8080/1-0/competitions/bblapi/2023:BBL
// curl localhost:8080/1-0/competitions/fifa/1jt5mxgn4q5r6mknmlqv5qjh0

////////////////////////////////////////////////////////////////////////////////
const checkSchema = function (schema) {
	if (!schema) return null;
	switch (schema.toLowerCase()) {
		case 'competitions':
			return 'competitions';
		case 'clubs':
			return 'clubs';
		case 'rankings':
			return 'rankings';
		case 'keymoments':
			return 'keyMoments';
		case 'sportspersons':
			return 'sportsPersons';
		case 'stages':
			return 'stages';
		case 'teams':
			return 'teams';
		case 'venues':
			return 'venues';
		case 'events':
			return 'events';
		case 'stickies':
			return 'stickies';
		case 'relationships':
			return 'relationships';
		case 'staff':
			return 'staff';
		case 'stories':
			return 'stories';
		case 'nations':
			return 'nations';
		case 'sgos':
			return 'sgos';
		default:
			return null;
	}
};

////////////////////////////////////////////////////////////////////////////////
async function getSingleSportsData(req, res) {
	const id = uuid.v4();
	debug(`${req.method} ${req.url}${req.hostname != undefined ? ' [called from ' + req.hostname + ']' : ''}`, id);
	const schemaType = req.params.schemaType;
	const scope = req.params.scope;
	const requestedId = req.params.id;
	const aggregation = req?.query?.aggregation ? req.query.aggregation.toLowerCase() : null;
	const schema = checkSchema(schemaType);
	const mongo = config?.mongo;

	//////////////////////////////////////////////////////////////////////////////
	if (!mongo || !mongo.db || !mongo.client) {
		warn(`No MongoDB connection available`, 'WD0050', 500, 'Database Connection Error');
		send400(res, {
			message: 'Database connection is not available.',
			errorCode: 'WD0050', // TODO: Error codes should be documented in a central location and not as magic numbers in code
			category: 'Database Connection Error',
		});
		return;
	}

	//////////////////////////////////////////////////////////////////////////////
	if (!schema) {
		warn(`No valid Schema found when trying to get: ${schemaType}`, 'WD0040', 400, 'Invalid Schema');
		send400(res, {
			message: 'Please specify a valid schema type.',
			errorCode: 'WD0040', // TODO: Error codes should be documented in a central location and not as magic numbers in code
			category: 'Invalid Schema',
		});
		return;
	}
	const report = `Fetching single sports data for schemaType: ${schemaType}, scope: ${scope}, id: ${requestedId} with query strings ${JSON.stringify(req.query)}`;

	//////////////////////////////////////////////////////////////////////////////
	try {
		const r = await mongo.db.collection(schema).findOne(scope.toLowerCase() !== 'gameday' ? { _externalIdScope: scope, _externalId: requestedId } : { _id: requestedId });
		if (r == null) {
			warn(`No data found for schemaType: ${schemaType}, scope: ${scope}, id: ${requestedId}`, 'WD0061', 404, 'Data Not Found');
			send404(res, {
				message: 'No data found for the specified parameters.',
				errorCode: 'WDxxx', // TODO: Error codes should be documented in a central location and not as magic numbers in code
				category: 'Data Not Found',
			});
			return;
		}

		info(report, id);

		////////////////////////////////////////////////////////////////////////////
		// We have an aggregation request - process it here
		if (aggregation) {
			const aggregationString = aggregation.toLowerCase();
			const aggregations = new Set();
			const requestedAggregationPaths = aggregation.split(',').map((a) => a.trim());
			for (const aggregationPath of requestedAggregationPaths) {
				const aggregationElements = aggregationPath.split('.').map((a) => a.trim());
				for (const element of aggregationElements) {
					aggregations.add(element);
				}
			}
			debug(`Requested aggregations: ${Array.from(aggregations).join(', ')}`, id);
			//////////////////////////////////////////////////////////////////////////
			if (r.resourceType && r.resourceType.toLowerCase() === 'competition' && aggregation.startsWith('cs')) {
				const aggregationsResult = await ManageCSAggregation(mongo, aggregations, r, aggregationString, id);
				send200(res, aggregationsResult);
				return;
			}
		} else {
			info(`No aggregation requested, returning raw data`, id);
			// TODO: add other standard processing here
			send200(res, r);
			return;
		}

		send200(res, r);
		return;
	} catch (e) {
		warn(`Error fetching single sports data for schemaType: ${schemaType}, scope: ${scope}, id: ${requestedId} - ${e.message}`, 'WD0060', 500, 'Database Query Error');
		send500(res, {
			message: `Error fetching data: ${e.message}`,
			errorCode: 'WDxxx', // TODO: Error codes should be documented in a central location and not as magic numbers in code
			category: 'Database Query Error',
		});
		return;
	}
}

////////////////////////////////////////////////////////////////////////////////
const pipelineLookupCompetitionStages = {
	$lookup: {
		from: 'stages',
		let: {
			compScope: '$_externalIdScope',
			compId: '$_externalId',
		},
		pipeline: [
			{
				$match: {
					$expr: {
						$and: [{ $eq: ['$_externalCompetitionIdScope', '$$compScope'] }, { $eq: ['$_externalCompetitionId', '$$compId'] }],
					},
				},
			},
			////////////////////////////////////////////////////////////////////////
			// Project only the required fields for stages
			{
				$project: {
					_id: 1,
					_externalIdScope: 1,
					_externalId: 1,
				},
			},
		],
		as: '_aggregation.stages',
	},
};
////////////////////////////////////////////////////////////////////////////////
const pipelineLookupStageEvents = {
	$lookup: {
		from: 'events',
		let: { stageScope: '$_externalIdScope', stageId: '$_externalId' },
		pipeline: [
			{
				$match: {
					$expr: {
						$and: [{ $eq: ['$_externalStageIdScope', '$$stageScope'] }, { $eq: ['$_externalStageId', '$$stageId'] }],
					},
				},
			},
			////////////////////////////////////////////////////////////////////////
			// Project only the required fields for events
			{
				$project: {
					_id: 1,
					_externalIdScope: 1,
					_externalId: 1,
					_externalVenueIdScope: 1,
					_externalVenueId: 1,
				},
			},
		],
		as: '_aggregation.events',
	},
};
////////////////////////////////////////////////////////////////////////////////
const pipelineLookupEventVenues = {
	$lookup: {
		from: 'venues',
		let: { venueScope: '$_externalVenueIdScope', venueId: '$_externalVenueId' },
		pipeline: [
			{
				$match: {
					$expr: {
						$and: [{ $eq: ['$_externalIdScope', '$$venueScope'] }, { $eq: ['$_externalId', '$$venueId'] }],
					},
				},
			},
			////////////////////////////////////////////////////////////////////////
			// Project only the required fields for venues
			{
				$project: {
					_id: 1,
					_externalIdScope: 1,
					_externalId: 1,
				},
			},
		],
		as: '_aggregation.venues',
	},
};
////////////////////////////////////////////////////////////////////////////////
const pipelineLookupEventKeyMoments = {
	$lookup: {
		from: 'keyMoments',
		let: { eventScope: '$_externalIdScope', eventId: '$_externalId' },
		pipeline: [
			{
				$match: {
					$expr: {
						$and: [{ $eq: ['$_externalEventIdScope', '$$eventScope'] }, { $eq: ['$_externalEventId', '$$eventId'] }],
					},
				},
			},
			{
				$project: {
					_id: 1,
				},
			},
		],
		as: '_aggregation.keyMoments',
	},
};

////////////////////////////////////////////////////////////////////////////////
// Manages: cs,se[.ev],ekm pipeline
// WARNING: Probably remove EKM from the pipeline as it will be too much data
async function ManageCSAggregationPipeline(competitionIdScope, competitionId, aggregationsSet, aggregationString) {
	if (!aggregationsSet.has(`cs`)) return null;
	const pipelineParts = {};
	///////////////////////////////////////////////////////////////////////////////
	pipelineParts.init = {
		$match: {
			_externalIdScope: competitionIdScope,
			_externalId: competitionId,
		},
	};
	///////////////////////////////////////////////////////////////////////////////
	// Lookup all stages that reference this competition
	pipelineParts.lookupStages = _.cloneDeep(pipelineLookupCompetitionStages);
	//////////////////////////////////////////////////////////////////////////////
	// Lookup all events that reference these stages - do we hve se join?
	if (aggregationsSet.has('se')) {
		pipelineParts.lookupStageEvents = _.cloneDeep(pipelineLookupStageEvents);
		pipelineParts.lookupStages.$lookup.pipeline.push(pipelineParts.lookupStageEvents);
	}
	////////////////////////////////////////////////////////////////////////////////
	// If we don't have se join, we can't do anything else - return what we have
	else {
		writeFileSyncWithDirs(`${config.cwd}/scratch/pipeline.${aggregationString}.jsonc`, JSON.stringify([pipelineParts.init, pipelineParts.lookupStages], null, 2));
		return [pipelineParts.init, pipelineParts.lookupStages];
	}
	//////////////////////////////////////////////////////////////////////////////
	// We have an se join - do we have ev or ekm joins?
	//////////////////////////////////////////////////////////////////////////////
	// Lookup all venues for the events - do we have ev join?
	if (aggregationsSet.has('se') && aggregationsSet.has('ev')) {
		pipelineParts.lookupEventVenues = _.cloneDeep(pipelineLookupEventVenues);
		pipelineParts.lookupStageEvents.$lookup.pipeline.push(pipelineParts.lookupEventVenues);
	}
	//////////////////////////////////////////////////////////////////////////////
	// Remove the _externalVenueIdScope and _externalVenueId from the events projection
	else {
		const eventProjection = pipelineParts.lookupStageEvents.$lookup.pipeline.find((p) => p.$project);
		if (eventProjection) {
			delete eventProjection.$project._externalVenueIdScope;
			delete eventProjection.$project._externalVenueId;
		}
	}
	//////////////////////////////////////////////////////////////////////////////
	// Lookup all key moments for the events - do we have km join?
	if (aggregationsSet.has('se') && aggregationsSet.has('ekm')) {
		pipelineParts.lookupEventKeyMoments = _.cloneDeep(pipelineLookupEventKeyMoments);
		pipelineParts.lookupStageEvents.$lookup.pipeline.push(pipelineParts.lookupEventKeyMoments);
	}

	//////////////////////////////////////////////////////////////////////////////
	// All done. Return the pipeline
	//////////////////////////////////////////////////////////////////////////////
	writeFileSyncWithDirs(`${config.cwd}/scratch/pipeline.${aggregationString}.jsonc`, JSON.stringify([pipelineParts.init, pipelineParts.lookupStages], null, 2));
	return [pipelineParts.init, pipelineParts.lookupStages];
}

////////////////////////////////////////////////////////////////////////////////
async function ManageCSAggregation(mongo, aggregationsSet, competitionDocument, aggregationString, requestId) {
	if (!aggregationsSet.has(`cs`)) return null; // No CS aggregation requested
	const competitionIdScope = competitionDocument._externalIdScope;
	const competitionId = competitionDocument._externalId;
	if (!competitionIdScope || !competitionId) {
		warn(`Cannot perform CS aggregation as competition document is missing _externalIdScope or _externalId`, requestId);
		return null;
	}
	//////////////////////////////////////////////////////////////////////////////
	const pipeline = await ManageCSAggregationPipeline(competitionIdScope, competitionId, aggregationsSet, aggregationString);
	if (!pipeline) return null;
	// Start timing
	const startTime = process.hrtime.bigint();
	debug(`Started Aggregation Pipeline for ${aggregationString}`, requestId);
	const result = await mongo.db.collection('competitions').aggregate(pipeline).toArray();
	// End timing and calculate duration
	const endTime = process.hrtime.bigint();
	const durationMs = Number(endTime - startTime) / 1_000_000; // Convert nanoseconds to milliseconds
	debug(`Finished Aggregation Pipeline for ${aggregationString} in ${durationMs.toFixed(2)}ms`, requestId);
	const stageIds = new Set();
	const eventIds = new Set();
	//////////////////////////////////////////////////////////////////////////////
	// Get competition stages
	for (const doc of result) {
		if (!Array.isArray(doc._aggregation?.cs)) continue;
		for (const stage of doc._aggregation.cs) {
			stageIds.add(stage);
		}
	}
	//////////////////////////////////////////////////////////////////////////////
	// Get Stage Events
	for (const stage of stageIds) {
		if (Array.isArray(stage?._aggregation?.se)) {
			for (const event of stage._aggregation.se) {
				eventIds.add(event);
			}
		}
	}
	return result;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { getSingleSportsData };
