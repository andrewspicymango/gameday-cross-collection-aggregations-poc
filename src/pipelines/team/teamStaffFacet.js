const { keySeparator, teamSeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const teamStaffFacet = [
	{
		$lookup: {
			from: 'staff',
			let: { externalTeamId: '$_externalId', externalTeamIdScope: '$_externalIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalTeamId', '$$externalTeamId'] }, { $eq: ['$_externalTeamIdScope', '$$externalTeamIdScope'] }] } } },
				{
					$project: {
						_id: 1,
						staffKey: {
							$cond: [
								{
									$and: [
										{ $eq: [{ $type: '$_externalTeamId' }, 'string'] },
										{ $ne: ['$_externalTeamId', ''] },
										{ $eq: [{ $type: '$_externalTeamIdScope' }, 'string'] },
										{ $ne: ['$_externalTeamIdScope', ''] },
										{ $eq: [{ $type: '$_externalSportsPersonId' }, 'string'] },
										{ $ne: ['$_externalSportsPersonId', ''] },
										{ $eq: [{ $type: '$_externalSportsPersonIdScope' }, 'string'] },
										{ $ne: ['$_externalSportsPersonIdScope', ''] },
									],
								},
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
								null,
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
			keys: { $cond: [{ $gt: [{ $size: '$staffDocs' }, 0] }, { $arrayToObject: { $map: { input: '$staffDocs', as: 's', in: ['$$s.staffKey', '$$s._id'] } } }, {}] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.teamStaffFacet = teamStaffFacet;
