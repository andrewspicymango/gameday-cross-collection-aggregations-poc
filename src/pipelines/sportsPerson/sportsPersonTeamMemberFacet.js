const { keySeparator } = require('../constants.js');

////////////////////////////////////////////////////////////////////////////////
const sportsPersonTeamMemberFacet = () => {
	return [
		{
			$lookup: {
				from: 'teams',
				let: { currentSportsPersonId: '$_externalId', currentSportsPersonIdScope: '$_externalIdScope' },
				pipeline: [
					{
						$match: {
							$expr: {
								$anyElementTrue: {
									$map: {
										input: { $ifNull: ['$members', []] },
										as: 'member',
										in: {
											$and: [
												{ $eq: ['$$member._externalSportsPersonId', '$$currentSportsPersonId'] },
												{ $eq: ['$$member._externalSportsPersonIdScope', '$$currentSportsPersonIdScope'] },
											],
										},
									},
								},
							},
						},
					},
					{ $project: { _id: 1, teamKey: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
				],
				as: 'teamDocs',
			},
		},
		{
			$project: {
				_id: 0,
				ids: { $map: { input: '$teamDocs', as: 'e', in: '$$e._id' } },
				keys: { $arrayToObject: { $map: { input: '$teamDocs', as: 't', in: ['$$t.teamKey', '$$t._id'] } } },
			},
		},
	];
};

module.exports = { sportsPersonTeamMemberFacet };
