////////////////////////////////////////////////////////////////////////////////
// Includes
const _ = require(`lodash`);
const uuid = require('uuid');
const { send200, send400, send404, send500 } = require('../utils/httpResponseUtils.js');
const { debug, info, warn } = require('../log.js');
const config = require('../config.js');
const competitionFullPipeline = require('../pipelines/competition-sgo-stage-event-venue-team-sportsperson.js');
const runPipeline = require('../pipelines/runPipeline.js');

////////////////////////////////////////////////////////////////////////////////
// Constants
const matAggCollectionName = `materialisedAggregations`;

////////////////////////////////////////////////////////////////////////////////
// Notes
// curl -X POST localhost:8080/1-0/aggregate/competitions/bblapi/2023:BBL?create=true
// curl -X POST localhost:8080/1-0/aggregate/competitions/fifa/1jt5mxgn4q5r6mknmlqv5qjh0?create=true

////////////////////////////////////////////////////////////////////////////////
/**
 * Map a provided schema name (case-insensitive) to the internal collection key.
 *
 * This helper normalizes the input schema name and returns the corresponding
 * collection identifier used by the application. If the input is falsy
 * (null/undefined/empty string) or does not match any known schema, the
 * function returns null.
 *
 * Recognized mappings:
 *  - "competitions"  => "competitions"
 *  - "clubs"         => "clubs"
 *  - "rankings"      => "rankings"
 *  - "keymoments"    => "keyMoments"
 *  - "sportspersons" => "sportsPersons"
 *  - "stages"        => "stages"
 *  - "teams"         => "teams"
 *  - "venues"        => "venues"
 *  - "events"        => "events"
 *  - "stickies"      => "stickies"
 *  - "relationships" => "relationships"
 *  - "staff"         => "staff"
 *  - "stories"       => "stories"
 *  - "nations"       => "nations"
 *  - "sgos"          => "sgos"
 *
 * @function checkSchema
 * @param {string|null|undefined} schema - The input schema name to normalize and match. Comparison is case-insensitive.
 * @returns {string|null} The normalized collection name if recognized; otherwise null.
 * @example
 * checkSchema('Clubs'); // => 'clubs'
 * @example
 * checkSchema('KeyMoments'); // => 'keyMoments'
 * @example
 * checkSchema('unknown'); // => null
 */
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
async function buildMaterialisedViewController(req, res) {
	const id = uuid.v4();
	debug(`${req.method} ${req.url}${req.hostname != undefined ? ' [called from ' + req.hostname + ']' : ''}`, id);
	const schemaType = req.params.schemaType;
	const scope = req.params.scope;
	const requestedId = req.params.id;
	const createResource = req?.query?.create && req.query.create === 'true' ? true : false;
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

	const report = `Creating materialised aggregation view of single sports data for schemaType: ${schemaType}, scope: ${scope}, id: ${requestedId} with query strings ${JSON.stringify(
		req.query
	)}`;
	info(report, id);

	try {
		switch (schemaType.toLowerCase()) {
			case 'competitions':
				if (createResource) await _getMongoCompetitionAggregatedView(mongo, scope, requestedId, id);
				// if (createResource) await _getMongoCompetitionStageAggregatedView(mongo, scope, requestedId, id);
				// if (createResource) await _getMongoCompetitionEventVenueAggregatedView(mongo, scope, requestedId, id);

				send200(res, { status: 200, service: config?.serviceName, message: `Materialised aggregation view created for ${schemaType} ${scope}/${requestedId}` }, config);
				return;
			case 'clubs':
			case 'rankings':
			case 'keymoments':
			case 'sportspersons':
			case 'stages':
			case 'teams':
			case 'venues':
			case 'events':
			case 'stickies':
			case 'relationships':
			case 'staff':
			case 'stories':
			case 'nations':
			case 'sgos':
				break;
			default:
				warn(`No valid Schema found when trying to get: ${schemaType}`, 'WD0040', 400, 'Invalid Schema');
				send400(res, {
					message: 'Please specify a valid schema type.',
					errorCode: 'WD0040', // TODO: Error codes should be documented in a central location and not as magic numbers in code
					category: 'Invalid Schema',
				});
				return;
		}
	} catch (err) {
		send500(res, err.message);
		return;
	}
	//////////////////////////////////////////////////////////////////////////////
}

////////////////////////////////////////////////////////////////////////////////
// Mongo Competition Stage materialized view
async function _getMongoCompetitionStageAggregatedView(mongo, competitionIdScope, competitionId, requestId) {
	//////////////////////////////////////////////////////////////////////////////
	/**
	 * Aggregation pipeline that gathers all stage document IDs for a specific external competition
	 * and upserts a single summary document into the `matAggCollectionName` collection.
	 *  - The pipeline produces one summary document per external competition/scope pair.
	 *  - Using whenMatched: "replace" ensures the target document exactly matches the projected shape.
	 */
	const pipeline = [
		//////////////////////////////////////////////////////////////////////////////
		// $match: Filters documents to only those that belong to the specified external competition
		// and scope (uses competitionId and competitionIdScope from outer scope).
		{ $match: { _externalCompetitionId: competitionId, _externalCompetitionIdScope: competitionIdScope } },
		////////////////////////////////////////////////////////////////////////////
		// $sort: Orders matched documents by their _id in ascending order so pushed stage IDs preserve chronological/insert order.
		{ $sort: { _id: 1 } },
		////////////////////////////////////////////////////////////////////////////
		// $group: Groups documents by the pair {_externalCompetitionId, _externalCompetitionIdScope} and accumulates an array "stages" containing each document's _id.
		{ $group: { _id: { id: '$_externalCompetitionId', scope: '$_externalCompetitionIdScope' }, stages: { $push: { _id: '$_id' } } } },
		////////////////////////////////////////////////////////////////////////////
		// $project: Shapes the summary document to:
		//   - remove the aggregation _id,
		//   - expose _externalCompetitionId and _externalCompetitionIdScope,
		//   - include the collected stages array,
		//   - add an "asOf" timestamp (using $$NOW),
		//   - add a literal "type" field set to "SetCompetitionXStage".
		{
			$project: {
				_id: 0,
				resourceType: { $literal: `competition` },
				_externalIdScope: '$_id.scope',
				_externalId: '$_id.id',
				targetType: { $literal: `stage` },
				stages: 1,
				updatedAt: '$$NOW',
			},
		},
		////////////////////////////////////////////////////////////////////////////
		// $merge: Writes the resulting summary document into the "rel_sets_competition_stages" collection,  matching on the two external id fields. If a match exists the document is replaced; if not, a new document is inserted.
		{ $merge: { into: matAggCollectionName, on: ['resourceType', '_externalIdScope', '_externalId', 'targetType'], whenMatched: 'replace', whenNotMatched: 'insert' } },
	];
	await runPipeline(mongo, 'stages', pipeline, requestId);
}

////////////////////////////////////////////////////////////////////////////////
async function _getMongoCompetitionEventVenueAggregatedView(mongo, competitionIdScope, competitionId, requestId) {
	const pipeline = [
		//////////////////////////////////////////////////////////////////////////////
		// 1) limit to the competition
		{ $match: { _externalCompetitionIdScope: competitionIdScope, _externalCompetitionId: competitionId } },
		////////////////////////////////////////////////////////////////////////////
		// 2) keep only the join keys
		{ $project: { compScope: '$_externalCompetitionIdScope', compId: '$_externalCompetitionId', stageId: '$_externalId', stageScope: '$_externalIdScope' } },
		////////////////////////////////////////////////////////////////////////////
		// 3) stages -> events
		{
			$lookup: {
				from: 'events',
				let: { sId: '$stageId', sScope: '$stageScope' },
				pipeline: [
					{
						$match: {
							$expr: {
								$and: [{ $eq: ['$_externalStageId', '$$sId'] }, { $eq: ['$_externalStageIdScope', '$$sScope'] }],
							},
						},
					},
					{ $project: { _id: 1, _externalVenueId: 1, _externalVenueIdScope: 1 } },
				],
				as: 'events',
			},
		},
		////////////////////////////////////////////////////////////////////////////
		// explode events (if a competition has stages but no events, nothing will be emitted)
		{ $unwind: { path: '$events', preserveNullAndEmptyArrays: true } },
		////////////////////////////////////////////////////////////////////////////
		// 4) events -> venues
		{
			$lookup: {
				from: 'venues',
				let: { vId: '$events._externalVenueId', vScope: '$events._externalVenueIdScope' },
				// join on venue foreign keys
				pipeline: [{ $match: { $expr: { $and: [{ $eq: ['$_externalId', '$$vId'] }, { $eq: ['$_externalIdScope', '$$vScope'] }] } } }, { $project: { _id: 1 } }],
				as: 'venue',
			},
		},
		////////////////////////////////////////////////////////////////////////////
		// 5) explode venues (if a competition has stages but no venues, nothing will be emitted)
		{ $unwind: { path: '$venue', preserveNullAndEmptyArrays: true } },
		////////////////////////////////////////////////////////////////////////////
		// 5) collect unique ids for both events and venues
		{ $group: { _id: { scope: '$compScope', id: '$compId' }, eventIds: { $addToSet: '$events._id' }, venueIds: { $addToSet: '$venue._id' } } },

		////////////////////////////////////////////////////////////////////////////
		// 6) final single document
		{
			$project: {
				_id: 0,
				resourceType: { $literal: 'competition' },
				_externalIdScope: '$_id.scope',
				_externalId: '$_id.id',
				targetType: { $literal: 'eventsAndVenues' },
				////////////////////////////////////////////////////////////////////////
				// strip nulls and wrap as {_id}
				events: {
					$map: {
						input: { $filter: { input: '$eventIds', as: 'e', cond: { $ne: ['$$e', null] } } },
						as: 'e',
						in: { _id: '$$e' },
					},
				},
				venues: {
					$map: {
						input: { $filter: { input: '$venueIds', as: 'v', cond: { $ne: ['$$v', null] } } },
						as: 'v',
						in: { _id: '$$v' },
					},
				},
				updatedAt: '$$NOW',
			},
		},
		////////////////////////////////////////////////////////////////////////////
		// 7) materialize (optional)
		{
			$merge: {
				into: 'materialisedAggregations',
				on: ['resourceType', '_externalIdScope', '_externalId', 'targetType'],
				whenMatched: 'replace',
				whenNotMatched: 'insert',
			},
		},
	];
	return await runPipeline(mongo, 'stages', pipeline, requestId);
}

////////////////////////////////////////////////////////////////////////////////
async function _getMongoCompetitionAggregatedView(mongo, competitionIdScope, competitionId, requestId) {
	if (!competitionId || !competitionIdScope) return null;
	const pipeline = competitionFullPipeline(competitionIdScope, competitionId);
	await runPipeline(mongo, 'competitions', pipeline, requestId);
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { buildMaterialisedViewController };
