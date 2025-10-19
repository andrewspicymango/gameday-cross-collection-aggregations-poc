const { keySeparator } = require('../constants.js');

////////////////////////////////////////////////////////////////////////////////
const sportsPersonKeyMomentFacet = () => {
	return [
		{
			$lookup: {
				from: 'keyMoments',
				let: { currentSportsPersonIdScope: '$_externalIdScope', currentSportsPersonId: '$_externalId' },
				pipeline: [
					{
						$match: {
							$expr: {
								$anyElementTrue: {
									$map: {
										input: '$participants',
										as: 'p',
										in: {
											$and: [
												{
													$eq: ['$$p._externalSportsPersonIdScope', '$$currentSportsPersonIdScope'],
												},
												{
													$eq: ['$$p._externalSportsPersonId', '$$currentSportsPersonId'],
												},
											],
										},
									},
								},
							},
						},
					},
					{
						$project: {
							_id: 1,
							keyMomentKey: {
								$concat: [
									{ $dateToString: { date: '$dateTime', format: '%Y-%m-%dT%H:%M:%S.%LZ' } },
									keySeparator,
									'$_externalEventId',
									keySeparator,
									'$_externalEventIdScope',
									keySeparator,
									'$type',
									keySeparator,
									'$subType',
								],
							},
						},
					},
				],
				as: 'keyMomentDocs',
			},
		},
		{
			$project: {
				_id: 0,
				ids: { $map: { input: '$keyMomentDocs', as: 'km', in: '$$km._id' } },
				keys: { $arrayToObject: { $map: { input: '$keyMomentDocs', as: 'km', in: ['$$km.keyMomentKey', '$$km._id'] } } },
			},
		},
	];
};

module.exports = { sportsPersonKeyMomentFacet };
