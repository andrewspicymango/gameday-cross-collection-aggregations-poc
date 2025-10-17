////////////////////////////////////////////////////////////////////////////////
const keyMomentMetaFacet = [
	{
		$project: {
			_id: 1,
			keyMomentDateTime: '$dateTime',
			keyMomentEventId: '$_externalEventId',
			keyMomentEventIdScope: '$_externalEventIdScope',
			keyMomentType: '$type',
			keyMomentSubType: '$subType',
			resourceType: { $toLower: '$resourceType' },
			name: {
				$cond: {
					if: { $eq: [{ $getField: { field: '$defaultLanguage', input: '$name' } }, null] },
					then: '$subType',
					else: { $getField: { field: '$defaultLanguage', input: '$name' } },
				},
			},
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.keyMomentMetaFacet = keyMomentMetaFacet;
