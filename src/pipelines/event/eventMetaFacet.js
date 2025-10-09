const eventMetaFacet = [
    {
        $project: {
            eventId: '$_externalId',
            eventIdScope: '$_externalIdScope',
            resourceType: { $literal: 'event' },
        },
    },
];

exports.eventMetaFacet = eventMetaFacet;