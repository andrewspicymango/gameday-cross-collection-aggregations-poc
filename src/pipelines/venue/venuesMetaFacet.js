////////////////////////////////////////////////////////////////////////////////
const venuesMetaFacet = [
	{
		$project: {
			_id: 1,
			venueId: '$_externalId',
			venueIdScope: '$_externalIdScope',
			resourceType: { $toLower: '$resourceType' },
			name: { $getField: { field: '$defaultLanguage', input: '$name' } },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.venuesMetaFacet = venuesMetaFacet;
