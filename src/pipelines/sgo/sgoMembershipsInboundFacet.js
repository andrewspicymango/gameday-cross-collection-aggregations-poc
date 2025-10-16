const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const sgoMembershipsInboundFacet = (collectionName = 'sgos') => [
	{
		$lookup: {
			from: collectionName,
			let: { currentSgoId: '$_externalId', currentSgoScope: '$_externalIdScope' },
			pipeline: [
				{
					$match: {
						$expr: {
							$anyElementTrue: {
								$map: {
									input: { $ifNull: ['$sgoMemberships', []] },
									as: 'm',
									in: { $and: [{ $eq: ['$$m._externalSgoId', '$$currentSgoId'] }, { $eq: ['$$m._externalSgoIdScope', '$$currentSgoScope'] }] },
								},
							},
						},
					},
				},
				{ $project: { _id: 1, sgoKey: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'sgoDocs',
		},
	},
	{
		$project: {
			_id: 0,
			ids: { $map: { input: '$sgoDocs', as: 's', in: '$$s._id' } },
			keys: { $arrayToObject: { $map: { input: '$sgoDocs', as: 's', in: ['$$s.sgoKey', '$$s._id'] } } },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
module.exports = { sgoMembershipsInboundFacet };
