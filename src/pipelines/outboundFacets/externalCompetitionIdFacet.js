const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const externalCompetitionIdFacet = [
	{
		$lookup: {
			from: 'competitions',
			let: { targetCompetitionId: '$_externalCompetitionId', targetCompetitionIdScope: '$_externalCompetitionIdScope' },
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [{ $eq: ['$_externalId', '$$targetCompetitionId'] }, { $eq: ['$_externalIdScope', '$$targetCompetitionIdScope'] }],
						},
					},
				},
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
				{ $set: { key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'competitions',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$competitions', as: 'c', in: '$$c._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$competitions' }, 0] }, { $arrayToObject: { $map: { input: '$competitions', as: 's', in: ['$$s.key', '$$s._id'] } } }, {}] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
module.exports = externalCompetitionIdFacet;
