const { keySeparator, clubSeparator, teamSeparator, nationSeparator } = require('../constants.js');

////////////////////////////////////////////////////////////////////////////////
const sportsPersonStaffFacet = () => {
	return [
		{
			$lookup: {
				from: 'staff',
				let: { currentSportsPersonId: '$_externalId', currentSportsPersonIdScope: '$_externalIdScope' },
				pipeline: [
					{
						$match: {
							$expr: { $and: [{ $eq: ['$_externalSportsPersonId', '$$currentSportsPersonId'] }, { $eq: ['$_externalSportsPersonIdScope', '$$currentSportsPersonIdScope'] }] },
						},
					},
					{
						$project: {
							_id: 1,
							staffKey: {
								$cond: [
									// IF A TEAM
									{ $and: [{ $ne: ['$_externalTeamId', null] }, { $ne: ['$_externalTeamIdScope', null] }] },
									// THEN
									{
										$concat: [
											'$_externalSportsPersonId',
											keySeparator,
											'$_externalSportsPersonIdScope',
											teamSeparator,
											'$_externalTeamId',
											keySeparator,
											'$_externalTeamIdScope',
										],
									},
									// ELSE
									{
										$cond: [
											// IF A CLUB
											{ $and: [{ $ne: ['$_externalClubId', null] }, { $ne: ['$_externalClubIdScope', null] }] },
											// THEN
											{
												$concat: [
													'$_externalSportsPersonId',
													keySeparator,
													'$_externalSportsPersonIdScope',
													clubSeparator,
													'$_externalClubId',
													keySeparator,
													'$_externalClubIdScope',
												],
											},
											// ELSE IF A NATION
											{
												$cond: [
													{ $and: [{ $ne: ['$_externalNationId', null] }, { $ne: ['$_externalNationIdScope', null] }] },
													{
														$concat: [
															'$_externalSportsPersonId',
															keySeparator,
															'$_externalSportsPersonIdScope',
															nationSeparator,
															'$_externalNationId',
															keySeparator,
															'$_externalNationIdScope',
														],
													},
													// ELSE set to NULL
													null,
												],
											},
										],
									},
								],
							},
						},
					},
					{ $match: { staffKey: { $ne: null } } },
				],
				as: 'staffDocs',
			},
		},
		{
			$project: {
				_id: 0,
				ids: { $setUnion: [{ $map: { input: '$staffDocs', as: 's', in: '$$s._id' } }, []] },
				keys: { $cond: [{ $gt: [{ $size: '$staffDocs' }, 0] }, { $arrayToObject: { $map: { input: '$staffDocs', as: 'sd', in: ['$$sd.staffKey', '$$sd._id'] } } }, {}] },
			},
		},
	];
};

module.exports = { sportsPersonStaffFacet };
