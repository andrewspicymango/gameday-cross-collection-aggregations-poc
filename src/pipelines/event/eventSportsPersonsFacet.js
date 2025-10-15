const { keySeparator } = require('../constants');

const eventSportsPersonsFacet = [
	{
		$project: {
			sportsPersonParticipants: {
				$setUnion: [
					{
						$map: {
							input: { $ifNull: ['$participants', []] },
							as: 'p',
							in: {
								$cond: [
									{
										$and: [
											{ $eq: [{ $type: '$$p._externalSportsPersonId' }, 'string'] },
											{ $ne: ['$$p._externalSportsPersonId', ''] },
											{ $eq: [{ $type: '$$p._externalSportsPersonIdScope' }, 'string'] },
											{ $ne: ['$$p._externalSportsPersonIdScope', ''] },
										],
									},
									{
										id: '$$p._externalSportsPersonId',
										scope: '$$p._externalSportsPersonIdScope',
										key: { $concat: ['$$p._externalSportsPersonId', keySeparator, '$$p._externalSportsPersonIdScope'] },
									},
									null,
								],
							},
						},
					},
					[],
				],
			},
		},
	},
	{ $project: { sportsPersonParticipants: { $filter: { input: '$sportsPersonParticipants', as: 'sp', cond: { $ne: ['$$sp', null] } } } } },
	{
		$lookup: {
			from: 'sportsPersons',
			let: { spKeys: { $setUnion: [{ $map: { input: '$sportsPersonParticipants', as: 'sp', in: '$$sp.key' } }, []] } },
			pipeline: [
				{ $match: { $expr: { $and: [{ $gt: [{ $size: '$$spKeys' }, 0] }, { $in: [{ $concat: ['$_externalId', keySeparator, '$_externalIdScope'] }, '$$spKeys'] }] } } },
				{ $project: { _id: 1, key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'sportsHits',
		},
	},
	{
		$project: {
			_id: 0,
			ids: { $setUnion: [{ $map: { input: '$sportsHits', as: 'h', in: '$$h._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$sportsHits' }, 0] }, { $arrayToObject: { $map: { input: '$sportsHits', as: 's', in: ['$$s.key', '$$s._id'] } } }, {}] },
		},
	},
];

exports.eventSportsPersonsFacet = eventSportsPersonsFacet;
