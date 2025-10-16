const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const externalNationIdFacet = [
	{
		$lookup: {
			from: 'nations',
			let: { nid: '$_externalNationId', nids: '$_externalNationIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalId', '$$nid'] }, { $eq: ['$_externalIdScope', '$$nids'] }] } } },
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
				{ $set: { key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'nations',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$nations', as: 'n', in: '$$n._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$nations' }, 0] }, { $arrayToObject: { $map: { input: '$nations', as: 'n', in: ['$$n.key', '$$n._id'] } } }, {}] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
module.exports = externalNationIdFacet;
