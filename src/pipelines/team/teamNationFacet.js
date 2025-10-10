const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const teamNationFacet = [
	{
		$lookup: {
			from: 'nations',
			let: { nid: '$_externalNationId', nids: '$_externalNationIdScope' },
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [{ $eq: ['$_externalId', '$$nid'] }, { $eq: ['$_externalIdScope', '$$nids'] }],
						},
					},
				},
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
			],
			as: 'nations',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$nations', as: 'n', in: '$$n._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$nations', as: 'n', in: { $concat: ['$$n._externalId', keySeparator, '$$n._externalIdScope'] } } }, []] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
module.exports = { teamNationFacet };
