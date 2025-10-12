////////////////////////////////////////////////////////////////////////////////
/**
 * MongoDB aggregation facet that projects and renames team metadata fields.
 * Projects _id, teamId (from _externalId), teamIdScope (from _externalIdScope),
 * and resourceType to expose canonical identifiers for downstream stages.
 * Intended to be used as a facet pipeline entry in team-related aggregations.
 * @constant {Array<Object>}
 */
const teamMetaFacet = [
	{
		$project: {
			_id: 1,
			teamId: '$_externalId',
			teamIdScope: '$_externalIdScope',
			resourceType: '$resourceType',
			name: { $getField: { field: '$defaultLanguage', input: '$name' } },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.teamMetaFacet = teamMetaFacet;
