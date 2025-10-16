const { staffMetaFacet } = require('./staffMetaFacet.js');
const { staffTeamsFacet } = require('./staffTeamsFacet.js');
const { staffClubsFacet } = require('./staffClubsFacet.js');
const { staffSportsPersonsFacet } = require('./staffSportsPersonsFacet.js');
const { keySeparator, keyInAggregation, teamSeparator, clubSeparator } = require('../constants.js');

////////////////////////////////////////////////////////////////////////////////
/**
 * Creates a MongoDB aggregation pipeline for staff data processing and materialization.
 *
 * Filters staff records by sports person ID/scope and either team or club ID/scope,
 * then aggregates related data using facets and projects the results into a
 * standardized format before merging into a materialized aggregations collection.
 *
 * @param {Object} config - Configuration object containing MongoDB collection settings
 * @param {string} SP_SCOPE - Sports person external ID scope for filtering
 * @param {string} SP_ID - Sports person external ID for filtering
 * @param {string} TEAM_SCOPE - Team external ID scope for filtering (can be null)
 * @param {string} TEAM_ID - Team external ID for filtering (can be null)
 * @param {string} CLUB_SCOPE - Club external ID scope for filtering (can be null)
 * @param {string} CLUB_ID - Club external ID for filtering (can be null)
 * @returns {Array} MongoDB aggregation pipeline array with match, facet, project, addFields, and merge stages
 *
 * @description The pipeline:
 * 1. Matches documents by sports person and team/club criteria
 * 2. Uses facets to aggregate meta, teams, clubs, and sports persons data
 * 3. Projects results with computed external keys and flattened arrays
 * 4. Adds lastUpdated timestamp and merges into materialized collection
 */
const pipeline = (config, SP_SCOPE, SP_ID, TEAM_SCOPE, TEAM_ID, CLUB_SCOPE, CLUB_ID) => [
	// Create a match stage to filter by sportsPerson ID and scope and a match against team ID
	// and scope (if not null) or (if either null) against club ID and scope (if not null)
	{
		$match: {
			_externalSportsPersonId: SP_ID,
			_externalSportsPersonIdScope: SP_SCOPE,
			$or: [
				{ $and: [{ _externalTeamId: { $ne: null } }, { _externalTeamIdScope: { $ne: null } }, { _externalTeamId: TEAM_ID }, { _externalTeamIdScope: TEAM_SCOPE }] },
				{
					$and: [
						{ _externalTeamId: { $in: [null, undefined] } },
						{ _externalTeamIdScope: { $in: [null, undefined] } },
						{ _externalClubId: { $ne: null } },
						{ _externalClubIdScope: { $ne: null } },
						{ _externalClubId: CLUB_ID },
						{ _externalClubIdScope: CLUB_SCOPE },
					],
				},
			],
		},
	},
	{
		$facet: {
			meta: staffMetaFacet,
			teams: staffTeamsFacet,
			clubs: staffClubsFacet,
			sportsPersons: staffSportsPersonsFacet,
		},
	},
	{
		$project: {
			resourceType: { $first: '$meta.resourceType' },
			externalKey: {
				$cond: [
					{ $and: [{ $ne: [{ $first: '$meta._externalTeamId' }, null] }, { $ne: [{ $first: '$meta._externalTeamIdScope' }, null] }] },
					{
						$concat: [
							{ $first: '$meta._externalSportsPersonId' },
							keySeparator,
							{ $first: '$meta._externalSportsPersonIdScope' },
							teamSeparator,
							{ $first: '$meta._externalTeamId' },
							keySeparator,
							{ $first: '$meta._externalTeamIdScope' },
						],
					},
					{
						$cond: [
							{
								$and: [{ $ne: [{ $first: '$meta._externalClubId' }, null] }, { $ne: [{ $first: '$meta._externalClubIdScope' }, null] }],
							},
							{
								$concat: [
									{ $first: '$meta._externalSportsPersonId' },
									keySeparator,
									{ $first: '$meta._externalSportsPersonIdScope' },
									clubSeparator,
									{ $first: '$meta._externalClubId' },
									keySeparator,
									{ $first: '$meta._externalClubIdScope' },
								],
							},
							null,
						],
					},
				],
			},
			gamedayId: { $first: '$meta._id' },
			_externalSportsPersonId: { $first: '$meta._externalSportsPersonId' },
			_externalSportsPersonIdScope: { $first: '$meta._externalSportsPersonIdScope' },
			_externalTeamId: { $first: '$meta._externalTeamId' },
			_externalTeamIdScope: { $first: '$meta._externalTeamIdScope' },
			_externalClubId: { $first: '$meta._externalClubId' },
			_externalClubIdScope: { $first: '$meta._externalClubIdScope' },
			name: { $first: '$meta.name' },
			teamIds: { $ifNull: [{ $first: '$teams.ids' }, []] },
			teamKeys: { $ifNull: [{ $first: '$teams.keys' }, []] },
			clubIds: { $ifNull: [{ $first: '$clubs.ids' }, []] },
			clubKeys: { $ifNull: [{ $first: '$clubs.keys' }, []] },
			sportsPersonIds: { $ifNull: [{ $first: '$sportsPersons.ids' }, []] },
			sportsPersonKeys: { $ifNull: [{ $first: '$sportsPersons.keys' }, []] },
		},
	},
	{
		$addFields: {
			lastUpdated: '$$NOW',
		},
	},
	{
		$merge: {
			into: config?.mongo?.matAggCollectionName || 'materialisedAggregations',
			on: keyInAggregation,
			whenMatched: 'replace',
			whenNotMatched: 'insert',
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
/**
 * Generates a query object for staff aggregation documents based on provided identifiers.
 *
 * @param {string} sportsPersonId - The unique identifier for the sports person
 * @param {string} sportsPersonIdScope - The scope/context for the sports person ID
 * @param {string} teamId - The unique identifier for the team (optional)
 * @param {string} teamIdScope - The scope/context for the team ID (optional)
 * @param {string} clubId - The unique identifier for the club (optional)
 * @param {string} clubIdScope - The scope/context for the club ID (optional)
 * @returns {Object|null} Returns a query object with resourceType and externalKey properties,
 *                        or null if neither team nor club identifiers are provided
 * @returns {string} returns.resourceType - Always set to 'staff'
 * @returns {string} returns.externalKey - Concatenated key using appropriate separators
 *
 * @description
 * Prioritizes team-based queries over club-based queries. If team ID and scope are provided,
 * creates a team-based external key. Otherwise, if club ID and scope are provided, creates
 * a club-based external key. Returns null if neither condition is met.
 */
function queryForStaffAggregationDoc(sportsPersonId, sportsPersonIdScope, teamId, teamIdScope, clubId, clubIdScope) {
	if (teamId && teamIdScope) {
		return {
			resourceType: 'staff',
			externalKey: `${sportsPersonId}${keySeparator}${sportsPersonIdScope}${teamSeparator}${teamId}${keySeparator}${teamIdScope}`,
		};
	}
	if (clubId && clubIdScope) {
		return {
			resourceType: 'staff',
			externalKey: `${sportsPersonId}${keySeparator}${sportsPersonIdScope}${clubSeparator}${clubId}${keySeparator}${clubIdScope}`,
		};
	}
	return null;
}

////////////////////////////////////////////////////////////////////////////////
const getAllStaffByTeamRegex = {
	$regex: {
		$concat: [
			'^.*', // sportsPersonId (any)
			keySeparator,
			'.*', // sportsPersonIdScope (any)
			teamSeparator,
			'$_externalId', // teamId from incoming doc
			keySeparator,
			'$_externalIdScope', // teamIdScope from incoming doc
			'$',
		],
	},
};

////////////////////////////////////////////////////////////////////////////////
const getAllStaffByClubRegex = {
	$regex: {
		$concat: [
			'^.*', // sportsPersonId (any)
			keySeparator,
			'.*', // sportsPersonIdScope (any)
			clubSeparator,
			'$_externalId', // clubId from incoming doc
			keySeparator,
			'$_externalIdScope', // clubIdScope from incoming doc
			'$',
		],
	},
};

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, queryForStaffAggregationDoc };
