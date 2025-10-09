const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * stageTeamsFacet
 *
 * A MongoDB aggregation facet (an array of pipeline stages) that, given a stage document
 * (with fields `_externalId` and `_externalIdScope`), resolves the unique set of teams that
 * participate in events belonging to that stage.
 *
 * Assumptions:
 * - A variable `keySeparator` is in scope (string) and is used to build composite keys of the form
 *   "<externalId><keySeparator><externalScope>".
 * - Source collections used: "events", "teams".
 * - External id fields used on documents:
 *   - stage: `_externalId`, `_externalIdScope`
 *   - event: `_externalStageId`, `_externalStageIdScope`, `participants` (each participant may contain
 *     `_externalTeamId`, `_externalTeamIdScope`, and optionally sports person ids)
 *   - team: `_externalId`, `_externalIdScope`
 *
 * Behavior (high level):
 * 1. Lookup "events" using the stage's external ID and scope, filtering only events whose
 *    `event._externalStageId + keySeparator + event._externalStageIdScope` matches the stage key.
 *    For each matching event:
 *    - Filter `participants` to only include team participants (participant has non-null
 *      `_externalTeamId` and `_externalTeamIdScope`, and is NOT a sports person).
 *    - Map each participant to an object { id, scope, key } where `key` is the composite team key.
 *    - Union all event participant team arrays to form a unique list of team pairs (id+scope+key).
 * 2. Lookup "teams" with the set of composite team keys to find matching team documents and
 *    retrieve their ObjectId `_id` and composite key.
 * 3. Project the final facet output as an object with:
 *    - ids: unique array of team ObjectIds (from teams._id) — [] if none found
 *    - keys: unique array of composite team keys (strings) — [] if none found
 *
 * Output shape (single document produced by this facet):
 * {
 *   ids: [ObjectId, ...],   // set of resolved team document _id values
 *   keys: [string, ...]     // set of composite external keys ( "<id><sep><scope>" )
 * }
 *
 * Notes:
 * - The facet returns arrays (ids and keys) and guarantees uniqueness using $setUnion.
 * - The events lookup aggregates across all matching events then reduces to a single array via
 *   grouping and $reduce, ensuring the pipeline returns at most one aggregated result to process.
 * - This facet is intended to be used inside a larger aggregation (e.g., a stage-level pipeline)
 *   where the current input document represents a single stage.
 *
 * @constant
 * @type {Array<Object>}
 */
const stageTeamsFacet = [
	{ $addFields: { stageKey: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
	{
		$lookup: {
			from: 'events',
			let: { stageKey: '$stageKey' },
			pipeline: [
				{ $match: { $expr: { $eq: [{ $concat: ['$_externalStageId', keySeparator, '$_externalStageIdScope'] }, '$$stageKey'] } } },
				{
					$project: {
						teamParticipants: {
							$setUnion: [
								{
									$map: {
										input: {
											$filter: {
												input: { $ifNull: ['$participants', []] },
												as: 'p',
												cond: {
													$and: [
														{ $ne: ['$$p._externalTeamId', null] },
														{ $ne: ['$$p._externalTeamIdScope', null] },
														{
															$not: {
																$and: [{ $eq: [{ $type: '$$p._externalSportsPersonId' }, 'string'] }, { $eq: [{ $type: '$$p._externalSportsPersonIdScope' }, 'string'] }],
															},
														},
													],
												},
											},
										},
										as: 'p',
										in: {
											id: '$$p._externalTeamId',
											scope: '$$p._externalTeamIdScope',
											key: { $concat: ['$$p._externalTeamId', keySeparator, '$$p._externalTeamIdScope'] },
										},
									},
								},
								[],
							],
						},
					},
				},
				{ $group: { _id: null, arrs: { $push: '$teamParticipants' } } },
				{ $project: { _id: 0, pairs: { $reduce: { input: '$arrs', initialValue: [], in: { $setUnion: ['$$value', '$$this'] } } } } },
				{ $limit: 1 },
			],
			as: 'eventTeamsAgg',
		},
	},
	{ $project: { teamPairs: { $ifNull: [{ $getField: { field: 'pairs', input: { $first: '$eventTeamsAgg' } } }, []] } } },
	{
		$lookup: {
			from: 'teams',
			let: { teamKeys: { $setUnion: [{ $map: { input: '$teamPairs', as: 't', in: '$$t.key' } }, []] } },
			pipeline: [
				{ $match: { $expr: { $and: [{ $gt: [{ $size: '$$teamKeys' }, 0] }, { $in: [{ $concat: ['$_externalId', keySeparator, '$_externalIdScope'] }, '$$teamKeys'] }] } } },
				{ $project: { _id: 1, key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'teamsHit',
		},
	},
	{
		$project: {
			_id: 0,
			ids: { $setUnion: [{ $map: { input: '$teamsHit', as: 't', in: '$$t._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$teamsHit', as: 't', in: '$$t.key' } }, []] },
		},
	},
];

exports.stageTeamsFacet = stageTeamsFacet;
