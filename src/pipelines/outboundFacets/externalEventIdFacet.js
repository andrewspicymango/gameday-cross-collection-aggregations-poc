const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const externalEventIdFacet = [
	{
		$lookup: {
			from: 'events',
			let: { eventId: '$_externalEventId', eventIdScope: '$_externalEventIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalId', '$$eventId'] }, { $eq: ['$_externalIdScope', '$$eventIdScope'] }] } } },
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
				{ $set: { key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'events',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$events', as: 's', in: '$$s._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$events' }, 0] }, { $arrayToObject: { $map: { input: '$events', as: 'ev', in: ['$$ev.key', '$$ev._id'] } } }, {}] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
module.exports = externalEventIdFacet;
