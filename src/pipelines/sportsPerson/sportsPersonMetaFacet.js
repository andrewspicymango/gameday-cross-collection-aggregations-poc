////////////////////////////////////////////////////////////////////////////////
const sportsPersonMetaFacet = () => [
	{
		$project: {
			_id: 1,
			sportsPersonId: '$_externalId',
			sportsPersonIdScope: '$_externalIdScope',
			resourceType: { $toLower: '$resourceType' },
			name: { $getField: { field: '$defaultLanguage', input: '$lastName' } },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.sportsPersonMetaFacet = sportsPersonMetaFacet;
