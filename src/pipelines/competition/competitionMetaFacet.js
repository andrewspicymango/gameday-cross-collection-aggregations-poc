////////////////////////////////////////////////////////////////////////////////
const competitionMetaFacet = [
	{
		$project: {
			_id: 1,
			competitionId: '$_externalId',
			competitionIdScope: '$_externalIdScope',
			resourceType: '$resourceType',
			name: { $getField: { field: '$defaultLanguage', input: '$name' } },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.competitionMetaFacet = competitionMetaFacet;
