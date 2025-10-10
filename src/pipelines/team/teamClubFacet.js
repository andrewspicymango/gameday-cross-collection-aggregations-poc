const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const teamClubFacet = [
	{
		$lookup: {
			from: 'clubs',
			let: { cid: '$_externalClubId', cids: '$_externalClubIdScope' },
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [{ $eq: ['$_externalId', '$$cid'] }, { $eq: ['$_externalIdScope', '$$cids'] }],
						},
					},
				},
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
			],
			as: 'clubs',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$clubs', as: 'c', in: '$$c._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$clubs', as: 'c', in: { $concat: ['$$c._externalId', keySeparator, '$$c._externalIdScope'] } } }, []] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
module.exports = { teamClubFacet };
