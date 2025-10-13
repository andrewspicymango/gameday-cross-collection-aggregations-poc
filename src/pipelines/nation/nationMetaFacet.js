////////////////////////////////////////////////////////////////////////////////
/**
 * Projects nation metadata into a normalized shape.
 * Produces one document with:
 *  - _id (gamedayId)
 *  - nationId / nationIdScope (external identity)
 *  - resourceType
 *  - name: language-specific selection using defaultLanguage
 */
const nationMetaFacet = [
    {
        $project: {
            _id: 1,
            nationId: '$_externalId',
            nationIdScope: '$_externalIdScope',
            resourceType: '$resourceType',
            name: { $getField: { field: '$defaultLanguage', input: '$name' } },
        },
    },
];

////////////////////////////////////////////////////////////////////////////////
exports.nationMetaFacet = nationMetaFacet;