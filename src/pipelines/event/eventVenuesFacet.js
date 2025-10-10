const { keySeparator } = require('../constants');

const eventVenuesFacet = [
	{
		$project: {
			venuePair: {
				$cond: [
					{
						$and: [
							{ $eq: [{ $type: '$_externalVenueId' }, 'string'] },
							{ $ne: ['$_externalVenueId', ''] },
							{ $eq: [{ $type: '$_externalVenueIdScope' }, 'string'] },
							{ $ne: ['$_externalVenueIdScope', ''] },
						],
					},
					{
						id: '$_externalVenueId',
						scope: '$_externalVenueIdScope',
						key: { $concat: ['$_externalVenueId', keySeparator, '$_externalVenueIdScope'] },
					},
					null,
				],
			},
		},
	},
	{
		$lookup: {
			from: 'venues',
			let: { vKey: '$venuePair.key' },
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [{ $ne: ['$$vKey', null] }, { $eq: [{ $concat: ['$_externalId', keySeparator, '$_externalIdScope'] }, '$$vKey'] }],
						},
					},
				},
				{ $project: { _id: 1, key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'venueHits',
		},
	},
	{
		$project: {
			_id: 0,
			ids: { $setUnion: [{ $map: { input: '$venueHits', as: 'v', in: '$$v._id' } }, []] },
			keys: { $setUnion: [{ $cond: [{ $ne: ['$venuePair', null] }, ['$venuePair.key'], []] }, []] },
		},
	},
];

exports.eventVenuesFacet = eventVenuesFacet;
