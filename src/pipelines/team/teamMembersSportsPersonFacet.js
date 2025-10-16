const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const teamMembersSportsPersonFacet = [
	{
		$project: {
			sportsPersonParticipants: {
				$setUnion: [
					{
						$map: {
							input: { $ifNull: ['$members', []] },
							as: 'm',
							in: {
								$cond: [
									{
										$and: [
											{ $eq: [{ $type: '$$m._externalSportsPersonId' }, 'string'] },
											{ $ne: ['$$m._externalSportsPersonId', ''] },
											{ $eq: [{ $type: '$$m._externalSportsPersonIdScope' }, 'string'] },
											{ $ne: ['$$m._externalSportsPersonIdScope', ''] },
										],
									},
									{
										id: '$$m._externalSportsPersonId',
										scope: '$$m._externalSportsPersonIdScope',
										key: { $concat: ['$$m._externalSportsPersonId', keySeparator, '$$m._externalSportsPersonIdScope'] },
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
	{
		$project: {
			sportsPersonParticipants: {
				$filter: {
					input: '$sportsPersonParticipants',
					as: 'sp',
					cond: { $ne: ['$$sp', null] },
				},
			},
		},
	},
	{
		$lookup: {
			from: 'sportsPersons',
			let: { spKeys: { $setUnion: [{ $map: { input: '$sportsPersonParticipants', as: 'sp', in: '$$sp.key' } }, []] } },
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [{ $gt: [{ $size: '$$spKeys' }, 0] }, { $in: [{ $concat: ['$_externalId', keySeparator, '$_externalIdScope'] }, '$$spKeys'] }],
						},
					},
				},
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

////////////////////////////////////////////////////////////////////////////////
module.exports = { teamMembersSportsPersonFacet };
