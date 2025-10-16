const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const externalClubIdFacet = [
	{
		$lookup: {
			from: 'clubs',
			let: { clubId: '$_externalClubId', clubIdScope: '$_externalClubIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalId', '$$clubId'] }, { $eq: ['$_externalIdScope', '$$clubIdScope'] }] } } },
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
				{ $set: { key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'clubs',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$clubs', as: 's', in: '$$s._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$clubs' }, 0] }, { $arrayToObject: { $map: { input: '$clubs', as: 'c', in: ['$$c.key', '$$c._id'] } } }, {}] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
module.exports = externalClubIdFacet;
