const staffMetaFacet = [
	{
		$project: {
			_id: 1,
			_externalTeamId: { $ifNull: ['$_externalTeamId', null] },
			_externalTeamIdScope: { $ifNull: ['$_externalTeamIdScope', null] },
			_externalClubId: { $ifNull: ['$_externalClubId', null] },
			_externalClubIdScope: { $ifNull: ['$_externalClubIdScope', null] },
			_externalNationId: { $ifNull: ['$_externalNationId', null] },
			_externalNationIdScope: { $ifNull: ['$_externalNationIdScope', null] },
			_externalSportsPersonId: { $ifNull: ['$_externalSportsPersonId', null] },
			_externalSportsPersonIdScope: { $ifNull: ['$_externalSportsPersonIdScope', null] },
			resourceType: { $literal: 'staff' },
			name: { $getField: { field: '$defaultLanguage', input: '$lastName' } },
		},
	},
];

exports.staffMetaFacet = staffMetaFacet;
