const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Team Events Facet
 *
 * Aggregation facet pipeline for resolving event references for a given team.
 *
 * This pipeline is intended to be executed in the context of a team document that exposes
 * the following fields on the current pipeline input doc:
 *   - _externalId
 *   - _externalIdScope
 *
 * Behaviour summary:
 *   1. Build the team key string using the global `keySeparator` value:
 *      "<_externalId><keySeparator><_externalIdScope>".
 *   2. Lookup event documents (collection: "events") that have this team as a participant
 *      by matching the derived team key against participant entries with _externalTeamId and
 *      _externalTeamIdScope properties in the participants array.
 *   3. From each matched event, create event pairs with id, scope, and composite key.
 *      Only events with valid _externalId and _externalIdScope are included.
 *   4. Project a final object containing:
 *        - ids: unique set of matched events._id (typically ObjectId)
 *        - keys: unique set of external key strings ("<id><keySeparator><scope>") for the events
 *
 * Important notes / assumptions:
 *   - A variable keySeparator must be available in the outer JS scope where this pipeline is defined;
 *     it is used to compose the stable composite keys in all lookups.
 *   - Only events with string-typed, non-empty _externalId and _externalIdScope are considered.
 *   - Team participation is determined by matching team keys in the participants array.
 *   - Uniqueness is enforced via $setUnion; the final projection produces de-duplicated arrays.
 *   - If no matches are found, the pipeline yields empty arrays for both ids and keys.
 *
 * Result shape (per team input document):
 *   {
 *     ids: [ /* Array of events._id values (unique) *\/ ],
 *     keys: [ /* Array of "<id><keySeparator><scope>" strings (unique) *\/ ]
 *   }
 *
 * Collections referenced:
 *   - "events"
 *
 * Usage:
 *   - Can be used as a facet or inline pipeline stage when aggregating teams to resolve
 *     the set of event references where the team participates.
 *
 * @constant
 * @type {Array<Object>}
 * @name teamEventsFacet
 */
const teamEventsFacet = [
	{ $addFields: { teamKey: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },

	{
		$lookup: {
			from: 'events',
			let: { teamKey: '$teamKey' },
			pipeline: [
				{
					$match: {
						$expr: {
							$gt: [
								{
									$size: {
										$filter: {
											input: { $ifNull: ['$participants', []] },
											as: 'p',
											cond: { $eq: [{ $concat: ['$$p._externalTeamId', keySeparator, '$$p._externalTeamIdScope'] }, '$$teamKey'] },
										},
									},
								},
								0,
							],
						},
					},
				},
				{
					$project: {
						_id: 1,
						eventPair: {
							$cond: [
								{
									$and: [
										{ $eq: [{ $type: '$_externalId' }, 'string'] },
										{ $ne: ['$_externalId', ''] },
										{ $eq: [{ $type: '$_externalIdScope' }, 'string'] },
										{ $ne: ['$_externalIdScope', ''] },
									],
								},
								{
									id: '$_externalId',
									scope: '$_externalIdScope',
									key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] },
								},
								null,
							],
						},
					},
				},
				{ $match: { eventPair: { $ne: null } } },
			],
			as: 'eventDocs',
		},
	},

	{
		$project: {
			_id: 0,
			ids: { $setUnion: [{ $map: { input: '$eventDocs', as: 'e', in: '$$e._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$eventDocs', as: 'e', in: '$$e.eventPair.key' } }, []] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.teamEventsFacet = teamEventsFacet;
