////////////////////////////////////////////////////////////////////////////////
/**
 * Aggregation facet pipeline that projects club metadata into a normalized shape.
 * Produces one stage ($project) mapping:
 *  - _id: original document id
 *  - clubId: value of _externalId
 *  - clubIdScope: value of _externalIdScope
 *  - resourceType: original resourceType
 *  - name: language-specific name chosen dynamically using the document's defaultLanguage via $getField.
 * Intended for use within a $facet to supply concise club identity data to downstream aggregations.
 * @constant
 * @type {Array<import('mongodb').Document>}
 */
const clubMetaFacet = [
	{
		$project: {
			_id: 1,
			clubId: '$_externalId',
			clubIdScope: '$_externalIdScope',
			resourceType: { $toLower: '$resourceType' },
			name: { $getField: { field: '$defaultLanguage', input: '$name' } },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.clubMetaFacet = clubMetaFacet;
