const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const externalVenueIdFacet = [
	{
		$lookup: {
			from: 'venues',
			let: { targetVenueId: '$_externalVenueId', targetVenueIdScope: '$_externalVenueIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalId', '$$targetVenueId'] }, { $eq: ['$_externalIdScope', '$$targetVenueIdScope'] }] } } },
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
				{ $set: { key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'venues',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$venues', as: 'v', in: '$$v._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$venues' }, 0] }, { $arrayToObject: { $map: { input: '$venues', as: 's', in: ['$$s.key', '$$s._id'] } } }, {}] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
module.exports = externalVenueIdFacet;
