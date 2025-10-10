const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const teamVenueFacet = [
	{
		$lookup: {
			from: 'venues',
			let: { vid: '$_externalVenueId', vids: '$_externalVenueIdScope' },
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [{ $eq: ['$_externalId', '$$vid'] }, { $eq: ['$_externalIdScope', '$$vids'] }],
						},
					},
				},
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
			],
			as: 'venues',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$venues', as: 'v', in: '$$v._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$venues', as: 'v', in: { $concat: ['$$v._externalId', keySeparator, '$$v._externalIdScope'] } } }, []] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
module.exports = { teamVenueFacet };
