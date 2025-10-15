const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const eventKeyMomentsFacet = [
	{
		$lookup: {
			from: 'keyMoments',
			let: { thisEventId: '$_externalId', thisEventIdScope: '$_externalIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalEventId', '$$thisEventId'] }, { $eq: ['$_externalEventIdScope', '$$thisEventIdScope'] }] } } },
				{
					$project: {
						_id: 1,
						key: {
							$concat: [
								{ $ifNull: [{ $toString: '$dateTime' }, ''] },
								keySeparator,
								{ $ifNull: ['$_externalEventIdScope', ''] },
								keySeparator,
								{ $ifNull: ['$_externalEventId', ''] },
								keySeparator,
								{ $ifNull: ['$type', ''] },
								keySeparator,
								{ $ifNull: ['$subType', ''] },
							],
						},
					},
				},
			],
			as: 'keyMoments',
		},
	},
	{
		$project: {
			_id: 0,
			ids: { $setUnion: [{ $map: { input: '$keyMoments', as: 'h', in: '$$h._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$keyMoments' }, 0] }, { $arrayToObject: { $map: { input: '$keyMoments', as: 's', in: ['$$s.key', '$$s._id'] } } }, {}] },
		},
	},
];

exports.eventKeyMomentsFacet = eventKeyMomentsFacet;
