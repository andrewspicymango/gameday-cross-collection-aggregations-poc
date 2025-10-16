const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const externalStageIdFacet = [
	{
		$lookup: {
			from: 'stages',
			let: { targetStageId: '$_externalStageId', targetStageIdScope: '$_externalStageIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalId', '$$targetStageId'] }, { $eq: ['$_externalIdScope', '$$targetStageIdScope'] }] } } },
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
				{ $set: { key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'stages',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$stages', as: 's', in: '$$s._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$stages' }, 0] }, { $arrayToObject: { $map: { input: '$stages', as: 's', in: ['$$s.key', '$$s._id'] } } }, {}] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
module.exports = externalStageIdFacet;
