const eventMetaFacet = [
	{
		$project: {
			_id: 1,
			eventId: '$_externalId',
			eventIdScope: '$_externalIdScope',
			resourceType: { $literal: 'event' },
		},
	},
];

exports.eventMetaFacet = eventMetaFacet;
