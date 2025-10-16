const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const externalTeamIdFromParticipantFacet = [
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
											{ $not: { $and: [{ $eq: [{ $type: '$$p._externalSportsPersonId' }, 'string'] }, { $eq: [{ $type: '$$p._externalSportsPersonIdScope' }, 'string'] }] } },
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
	{
		$lookup: {
			from: 'teams',
			let: { teamKeys: { $setUnion: [{ $map: { input: '$teamParticipants', as: 't', in: '$$t.key' } }, []] } },
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
			keys: { $cond: [{ $gt: [{ $size: '$teamsHit' }, 0] }, { $arrayToObject: { $map: { input: '$teamsHit', as: 's', in: ['$$s.key', '$$s._id'] } } }, {}] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
module.exports = externalTeamIdFromParticipantFacet;
