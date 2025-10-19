const { keySeparator, rankingLabelSeparator, rankingPositionSeparator, rankingEventTeamSeparator, rankingEventSportsPersonSeparator } = require('../constants.js');

////////////////////////////////////////////////////////////////////////////////
const eventRankingsFacet = [
	{
		$lookup: {
			from: 'rankings',
			let: { thisEventId: '$_externalId', thisEventIdScope: '$_externalIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalEventId', '$$thisEventId'] }, { $eq: ['$_externalEventIdScope', '$$thisEventIdScope'] }] } } },
				{
					$project: {
						_id: 1,
						_externalEventId: 1,
						_externalEventIdScope: 1,
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

////////////////////////////////////////////////////////////////////////////////
exports.eventRankingsFacet = eventRankingsFacet;
