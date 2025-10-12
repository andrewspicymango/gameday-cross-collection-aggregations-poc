const eventMetaFacet = [
	{
		$project: {
			_id: 1,
			eventId: '$_externalId',
			eventIdScope: '$_externalIdScope',
			resourceType: { $literal: 'event' },
			name: { $getField: { field: '$defaultLanguage', input: '$name' } },
		},
	},
];

exports.eventMetaFacet = eventMetaFacet;
