const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const venuesInboundFacet = (collectionName) => {
	if (!collectionName) throw new Error('venuesInboundFacet requires a collectionName');
	return [
		{
			$lookup: {
				from: collectionName,
				let: { currentVenueId: '$_externalId', currentVenueScope: '$_externalIdScope' },
				pipeline: [
					{ $match: { $expr: { $and: [{ $eq: ['$_externalVenueId', '$$currentVenueId'] }, { $eq: ['$_externalVenueIdScope', '$$currentVenueScope'] }] } } },
					{ $project: { _id: 1, venueKey: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
					{ $match: { venueKey: { $ne: null } } },
				],
				as: 'venueDocs',
			},
		},
		{
			$project: {
				_id: 0,
				ids: { $setUnion: [{ $map: { input: '$venueDocs', as: 's', in: '$$s._id' } }, []] },
				keys: { $cond: [{ $gt: [{ $size: '$venueDocs' }, 0] }, { $arrayToObject: { $map: { input: '$venueDocs', as: 'v', in: ['$$v.venueKey', '$$v._id'] } } }, {}] },
			},
		},
	];
};

module.exports = { venuesInboundFacet };
