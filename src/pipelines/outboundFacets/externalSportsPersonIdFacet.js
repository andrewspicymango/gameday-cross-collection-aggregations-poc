const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const staffSportsPersonsFacet = [
	{
		$lookup: {
			from: 'sportsPersons',
			let: { sportsPersonId: '$_externalSportsPersonId', sportsPersonIdScope: '$_externalSportsPersonIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalId', '$$sportsPersonId'] }, { $eq: ['$_externalIdScope', '$$sportsPersonIdScope'] }] } } },
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
				{ $set: { key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'sportsPersons',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$sportsPersons', as: 's', in: '$$s._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$sportsPersons' }, 0] }, { $arrayToObject: { $map: { input: '$sportsPersons', as: 's', in: ['$$s.key', '$$s._id'] } } }, {}] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
module.exports = staffSportsPersonsFacet;
