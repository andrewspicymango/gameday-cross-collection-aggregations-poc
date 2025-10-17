const eventMetaFacet = [
	{
		$project: {
			_id: 1,
			eventId: '$_externalId',
			eventIdScope: '$_externalIdScope',
			resourceType: { $toLower: '$resourceType' },
			name: { $getField: { field: '$defaultLanguage', input: '$name' } },
		},
	},
];

exports.eventMetaFacet = eventMetaFacet;
