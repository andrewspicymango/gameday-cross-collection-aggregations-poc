const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const externalTeamIdFacet = [
	{
		$lookup: {
			from: 'teams',
			let: { teamId: '$_externalTeamId', teamIdScope: '$_externalTeamIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalId', '$$teamId'] }, { $eq: ['$_externalIdScope', '$$teamIdScope'] }] } } },
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
				{ $set: { key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'teams',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$teams', as: 's', in: '$$s._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$teams' }, 0] }, { $arrayToObject: { $map: { input: '$teams', as: 't', in: ['$$t.key', '$$t._id'] } } }, {}] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
module.exports = externalTeamIdFacet;
