const { keySeparator, rankingLabelSeparator, rankingPositionSeparator, rankingStageTeamSeparator, rankingEventTeamSeparator } = require('../constants.js');

////////////////////////////////////////////////////////////////////////////////
/**
 * MongoDB aggregation pipeline facet for retrieving team rankings data.
 *
 * Performs a lookup operation to join team documents with their corresponding rankings
 * from the 'rankings' collection. Matches teams based on external ID and scope,
 * then constructs unique keys for each ranking record.
 *
 * The pipeline:
 * 1. Looks up rankings for each team using external team ID and scope
 * 2. Projects relevant ranking fields including IDs, datetime, and ranking position
 * 3. Generates composite keys based on stage/event context with separators
 * 4. Filters out records without valid keys
 * 5. Returns arrays of ranking IDs and key-to-ID mappings
 *
 * Key format patterns:
 * - Stage rankings: stageId|scope::teamId|scope||datetime::position
 * - Event rankings: eventId|scope::teamId|scope||datetime::position
 *
 * @type {Array<Object>} MongoDB aggregation pipeline stages
 * @returns {Object} Object containing 'ids' array and 'keys' object mapping
 */
const teamRankingsFacet = [
	{
		$lookup: {
			from: 'rankings',
			let: { thisTeamId: '$_externalId', thisTeamIdScope: '$_externalIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalTeamId', '$$thisTeamId'] }, { $eq: ['$_externalTeamIdScope', '$$thisTeamIdScope'] }] } } },
				{
					$project: {
						_id: 1,
						_externalStageId: 1,
						_externalStageIdScope: 1,
						_externalEventId: 1,
						_externalEventIdScope: 1,
						_externalTeamId: 1,
						_externalTeamIdScope: 1,
						dateTime: 1,
						ranking: 1,
					},
				},
				{
					$set: {
						key: {
							$cond: [
								{
									$and: [
										{ $ne: ['$_externalStageId', null] },
										{ $ne: ['$_externalStageIdScope', null] },
										{ $ne: ['$_externalTeamId', null] },
										{ $ne: ['$_externalTeamIdScope', null] },
									],
								},
								{
									$concat: [
										'$_externalStageId',
										keySeparator,
										'$_externalStageIdScope',
										rankingStageTeamSeparator,
										'$_externalTeamId',
										keySeparator,
										'$_externalTeamIdScope',
										rankingLabelSeparator,
										'$dateTime',
										rankingPositionSeparator,
										{ $toString: '$ranking' },
									],
								},
								{
									$cond: [
										{
											$and: [
												{ $ne: ['$_externalEventId', null] },
												{ $ne: ['$_externalEventIdScope', null] },
												{ $ne: ['$_externalTeamId', null] },
												{ $ne: ['$_externalTeamIdScope', null] },
											],
										},
										{
											$concat: [
												'$_externalEventId',
												keySeparator,
												'$_externalEventIdScope',
												rankingEventTeamSeparator,
												'$_externalTeamId',
												keySeparator,
												'$_externalTeamIdScope',
												rankingLabelSeparator,
												'$dateTime',
												rankingPositionSeparator,
												{ $toString: '$ranking' },
											],
										},
										null,
									],
								},
							],
						},
					},
				},
				{ $match: { key: { $ne: null } } },
			],
			as: 'rankings',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$rankings', as: 'r', in: '$$r._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$rankings' }, 0] }, { $arrayToObject: { $map: { input: '$rankings', as: 'r', in: ['$$r.key', '$$r._id'] } } }, {}] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.teamRankingsFacet = teamRankingsFacet;
