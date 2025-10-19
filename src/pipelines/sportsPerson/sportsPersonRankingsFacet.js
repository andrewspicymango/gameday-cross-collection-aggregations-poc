const { keySeparator, rankingLabelSeparator, rankingPositionSeparator, rankingStageSportsPersonSeparator, rankingEventSportsPersonSeparator } = require('../constants.js');

////////////////////////////////////////////////////////////////////////////////
/**
 * Creates an aggregation pipeline facet for retrieving sports person rankings.
 *
 * This function generates a MongoDB aggregation pipeline that:
 * 1. Performs a lookup join with the 'rankings' collection based on external sports person ID and scope
 * 2. Filters and projects ranking documents to include relevant fields
 * 3. Creates composite keys for rankings based on stage/event, sports person, datetime, and ranking position
 * 4. Returns both an array of ranking IDs and a key-value object mapping composite keys to ranking IDs
 *
 * The composite keys are generated using different separators:
 * - For stage-based rankings: stageId + scope + sportsPerson + datetime + ranking
 * - For event-based rankings: eventId + scope + sportsPerson + datetime + ranking
 *
 * @returns {Array} MongoDB aggregation pipeline array with $lookup and $project stages
 * @example
 * // Returns pipeline for aggregating sports person rankings
 * const pipeline = sportsPersonRankingsFacet();
 * // Use in aggregation: db.collection.aggregate([{ $facet: { rankings: pipeline } }])
 */
const sportsPersonRankingsFacet = () => {
	return [
		{
			$lookup: {
				from: 'rankings',
				let: { thisSportsPersonId: '$_externalId', thisSportsPersonIdScope: '$_externalIdScope' },
				pipeline: [
					{
						$match: {
							$expr: { $and: [{ $eq: ['$_externalSportsPersonId', '$$thisSportsPersonId'] }, { $eq: ['$_externalSportsPersonIdScope', '$$thisSportsPersonIdScope'] }] },
						},
					},
					{
						$project: {
							_id: 1,
							_externalStageId: 1,
							_externalStageIdScope: 1,
							_externalEventId: 1,
							_externalEventIdScope: 1,
							_externalSportsPersonId: 1,
							_externalSportsPersonIdScope: 1,
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
											{ $ne: ['$_externalSportsPersonId', null] },
											{ $ne: ['$_externalSportsPersonIdScope', null] },
										],
									},
									{
										$concat: [
											'$_externalStageId',
											keySeparator,
											'$_externalStageIdScope',
											rankingStageSportsPersonSeparator,
											'$_externalSportsPersonId',
											keySeparator,
											'$_externalSportsPersonIdScope',
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
													{ $ne: ['$_externalSportsPersonId', null] },
													{ $ne: ['$_externalSportsPersonIdScope', null] },
												],
											},
											{
												$concat: [
													'$_externalEventId',
													keySeparator,
													'$_externalEventIdScope',
													rankingEventSportsPersonSeparator,
													'$_externalSportsPersonId',
													keySeparator,
													'$_externalSportsPersonIdScope',
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
};

////////////////////////////////////////////////////////////////////////////////
exports.sportsPersonRankingsFacet = sportsPersonRankingsFacet;
