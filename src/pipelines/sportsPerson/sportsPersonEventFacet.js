const { keySeparator } = require('../constants.js');

////////////////////////////////////////////////////////////////////////////////
const sportsPersonEventFacet = () => {
	return [
		{
			$lookup: {
				from: 'events',
				let: { currentSportsPersonId: '$_externalId', currentSportsPersonIdScope: '$_externalIdScope' },
				pipeline: [
					{
						$match: {
							$expr: {
								$anyElementTrue: {
									$map: {
										input: { $ifNull: ['$participants', []] },
										as: 'participant',
										in: {
											$and: [
												{ $eq: ['$$participant._externalSportsPersonId', '$$currentSportsPersonId'] },
												{ $eq: ['$$participant._externalSportsPersonIdScope', '$$currentSportsPersonIdScope'] },
											],
										},
									},
								},
							},
						},
					},
					{ $project: { _id: 1, eventKey: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
				],
				as: 'eventDocs',
			},
		},
		{
			$project: {
				_id: 0,
				ids: { $map: { input: '$eventDocs', as: 'e', in: '$$e._id' } },
				keys: { $arrayToObject: { $map: { input: '$eventDocs', as: 'e', in: ['$$e.eventKey', '$$e._id'] } } },
			},
		},
	];
};

module.exports = { sportsPersonEventFacet };
