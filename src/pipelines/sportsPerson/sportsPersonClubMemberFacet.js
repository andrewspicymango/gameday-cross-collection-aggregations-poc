const { keySeparator } = require('../constants.js');

////////////////////////////////////////////////////////////////////////////////
const sportsPersonClubMemberFacet = () => {
	return [
		{
			$lookup: {
				from: 'clubs',
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
					{ $project: { _id: 1, clubKey: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
				],
				as: 'clubDocs',
			},
		},
		{
			$project: {
				_id: 0,
				ids: { $map: { input: '$clubDocs', as: 'e', in: '$$e._id' } },
				keys: { $arrayToObject: { $map: { input: '$clubDocs', as: 'c', in: ['$$c.clubKey', '$$c._id'] } } },
			},
		},
	];
};

module.exports = { sportsPersonClubMemberFacet };
