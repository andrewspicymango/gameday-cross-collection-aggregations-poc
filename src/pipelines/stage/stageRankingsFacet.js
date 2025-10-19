const { keySeparator, rankingLabelSeparator, rankingPositionSeparator, rankingStageTeamSeparator, rankingStageSportsPersonSeparator } = require('../constants.js');

////////////////////////////////////////////////////////////////////////////////
const stageRankingsFacet = [
	{
		$lookup: {
			from: 'rankings',
			let: { thisStageId: '$_externalId', thisStageIdScope: '$_externalIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalStageId', '$$thisStageId'] }, { $eq: ['$_externalStageIdScope', '$$thisStageIdScope'] }] } } },
				{
					$project: {
						_id: 1,
						_externalStageId: 1,
						_externalStageIdScope: 1,
						_externalTeamId: 1,
						_externalTeamIdScope: 1,
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
exports.stageRankingsFacet = stageRankingsFacet;
