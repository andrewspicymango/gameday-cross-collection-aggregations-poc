const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * MongoDB aggregation pipeline that creates a facet for team events lookup.
 *
 * This pipeline performs the following operations:
 * 1. Adds a teamKey field by concatenating team's _externalId and _externalIdScope
 * 2. Performs a lookup on the 'events' collection to find events where the team participates
 * 3. Matches events where the teamKey exists in the participants array
 * 4. Projects event data with eventPair containing id, scope, and key
 * 5. Filters out events without valid eventPair data
 * 6. Returns deduplicated event IDs and a key-to-ID mapping object
 *
 * @type {Array<Object>} MongoDB aggregation pipeline stages
 * @requires keySeparator - Global variable used to join external ID components
 *
 * @returns {Object} Result contains 'ids' array and 'keys' object mapping
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
							$in: [
								'$$teamKey',
								{ $map: { input: { $ifNull: ['$participants', []] }, as: 'p', in: { $concat: ['$$p._externalTeamId', keySeparator, '$$p._externalTeamIdScope'] } } },
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
			keys: { $cond: [{ $gt: [{ $size: '$eventDocs' }, 0] }, { $arrayToObject: { $map: { input: '$eventDocs', as: 'e', in: ['$$e.eventPair.key', '$$e._id'] } } }, {}] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.teamEventsFacet = teamEventsFacet;
