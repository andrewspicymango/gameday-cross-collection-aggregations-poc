const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * SGO Meta Facet
 *
 * Aggregation facet pipeline for extracting core metadata from an SGO document.
 *
 * This pipeline is intended to be executed in the context of an SGO document that exposes
 * standard fields like _id, _externalId, _externalIdScope, and name.
 *
 * Behaviour summary:
 *   1. Projects core SGO metadata including internal ID, external identifiers, and name
 *   2. Constructs a composite external key using "<_externalId><keySeparator><_externalIdScope>"
 *   3. Creates a standardized metadata object for consistent cross-resource referencing
 *
 * Important notes / assumptions:
 *   - A variable keySeparator must be available in the outer JS scope where this pipeline is defined
 *   - The SGO document is expected to have _id, _externalId, _externalIdScope, and name fields
 *   - The externalKey format matches the standard used across all resource types
 *
 * Result shape (per SGO input document):
 *   {
 *     gamedayId: /* SGO._id (typically ObjectId) *\/,
 *     externalKey: /* "<_externalId><keySeparator><_externalIdScope>" string *\/,
 *     resourceType: "sgo",
 *     name: /* SGO.name string *\/
 *   }
 *
 * Usage:
 *   - Used as the foundational facet in SGO aggregation pipelines to establish
 *     core document identity and metadata structure
 *
 * @constant
 * @type {Array<Object>}
 * @name sgoMetaFacet
 */
const sgoMetaFacet = [
	{
		$project: {
			gamedayId: '$_id',
			externalKey: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] },
			resourceType: { $literal: 'sgo' },
			name: { $getField: { field: '$defaultLanguage', input: '$name' } },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.sgoMetaFacet = sgoMetaFacet;
