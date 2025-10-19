const rankingMetaFacet = [
	{
		$project: {
			_id: 1,
			_externalSportsPersonId: { $ifNull: ['$_externalSportsPersonId', null] },
			_externalSportsPersonIdScope: { $ifNull: ['$_externalSportsPersonIdScope', null] },
			_externalTeamId: { $ifNull: ['$_externalTeamId', null] },
			_externalTeamIdScope: { $ifNull: ['$_externalTeamIdScope', null] },
			_externalStageId: { $ifNull: ['$_externalStageId', null] },
			_externalStageIdScope: { $ifNull: ['$_externalStageIdScope', null] },
			_externalEventId: { $ifNull: ['$_externalEventId', null] },
			_externalEventIdScope: { $ifNull: ['$_externalEventIdScope', null] },
			dateTime: { $ifNull: ['$dateTime', null] },
			ranking: { $toDouble: { $ifNull: ['$ranking', null] } },
			resourceType: { $toLower: '$resourceType' },
			name: { $getField: { field: '$defaultLanguage', input: '$name' } },
		},
	},
];

exports.rankingMetaFacet = rankingMetaFacet;
