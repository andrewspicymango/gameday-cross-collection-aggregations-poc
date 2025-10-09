const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * teamsFacet
 *
 * A MongoDB aggregation facet (an array of pipeline stages) that, given a competition document
 * (with fields `_externalId` and `_externalIdScope`), resolves the unique set of teams that
 * participate in events belonging to any stage of that competition.
 *
 * Assumptions:
 * - A variable `keySeparator` is in scope (string) and is used to build composite keys of the form
 *   "<externalId><keySeparator><externalScope>".
 * - Source collections used: "stages", "events", "teams".
 * - External id fields used on documents:
 *   - competition: `_externalId`, `_externalIdScope`
 *   - stage: `_externalId`, `_externalIdScope`, `_externalCompetitionId`, `_externalCompetitionIdScope`
 *   - event: `_externalStageId`, `_externalStageIdScope`, `participants` (each participant may contain
 *     `_externalTeamId`, `_externalTeamIdScope`, and optionally sports person ids)
 *   - team: `_externalId`, `_externalIdScope`
 *
 * Behavior (high level):
 * 1. Lookup "stages" to find stages that belong to the competition, projecting a composite
 *    stageKey = `stage._externalId + keySeparator + stage._externalIdScope`.
 * 2. Build `stageKeys` as a unique set of those composite stage keys.
 * 3. Lookup "events" using `stageKeys`, filtering only events whose composite
 *    `event._externalStageId + keySeparator + event._externalStageIdScope` is in `stageKeys`.
 *    For each matching event:
 *    - Filter `participants` to only include team participants (participant has non-null
 *      `_externalTeamId` and `_externalTeamIdScope`, and is NOT a sports person).
 *    - Map each participant to an object { id, scope, key } where `key` is the composite team key.
 *    - Union all event participant team arrays to form a unique list of team pairs (id+scope+key).
 * 4. Lookup "teams" with the set of composite team keys to find matching team documents and
 *    retrieve their ObjectId `_id` and composite key.
 * 5. Project the final facet output as an object with:
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
 * - This facet is intended to be used inside a larger aggregation (e.g., a competition-level pipeline)
 *   where the current input document represents a single competition.
 *
 * @constant
 * @type {Array<Object>}
 */
const competitionTeamsFacet = [
	{
		$lookup: {
			from: 'stages',
			let: { cid: '$_externalId', cs: '$_externalIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalCompetitionId', '$$cid'] }, { $eq: ['$_externalCompetitionIdScope', '$$cs'] }] } } },
				{ $project: { _id: 0, stageKey: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'stages',
		},
	},
	{ $project: { stageKeys: { $setUnion: [{ $map: { input: '$stages', as: 's', in: '$$s.stageKey' } }, []] } } },
	{
		$lookup: {
			from: 'events',
			let: { stageKeys: '$stageKeys' },
			pipeline: [
				{
					$match: {
						$expr: { $and: [{ $gt: [{ $size: '$$stageKeys' }, 0] }, { $in: [{ $concat: ['$_externalStageId', keySeparator, '$_externalStageIdScope'] }, '$$stageKeys'] }] },
					},
				},
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
			keys: { $setUnion: [{ $map: { input: '$teamPairs', as: 't', in: '$$t.key' } }, []] },
		},
	},
];
exports.competitionTeamsFacet = competitionTeamsFacet;
