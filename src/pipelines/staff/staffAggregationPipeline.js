const { staffMetaFacet } = require('./staffMetaFacet.js');
const { staffTeamsFacet } = require('./staffTeamsFacet.js');
const { staffClubsFacet } = require('./staffClubsFacet.js');
const { staffNationsFacet } = require('./staffNationsFacet.js');
const { staffSportsPersonsFacet } = require('./staffSportsPersonsFacet.js');
const { keySeparator, keyInAggregation, teamSeparator, clubSeparator, nationSeparator } = require('../constants.js');

////////////////////////////////////////////////////////////////////////////////
/**
 * Creates a MongoDB aggregation pipeline for staff data processing and materialization.
 *
 * This pipeline matches staff records by sports person ID and scope, then performs faceted
 * aggregation to collect related teams, clubs, nations, and sports persons data. The results
 * are projected into a standardized format and merged into a materialized aggregations collection.
 *
 * @param {Object} config - Configuration object containing MongoDB settings
 * @param {string} SP_SCOPE - Sports person ID scope for matching
 * @param {string} SP_ID - Sports person ID for matching
 * @param {string} TEAM_SCOPE - Team ID scope for key generation
 * @param {string} TEAM_ID - Team ID for key generation
 * @param {string} CLUB_SCOPE - Club ID scope for key generation
 * @param {string} CLUB_ID - Club ID for key generation
 * @param {string} NATION_SCOPE - Nation ID scope for key generation
 * @param {string} NATION_ID - Nation ID for key generation
 * @returns {Array} MongoDB aggregation pipeline array with $match, $facet, $project, $addFields, and $merge stages
 */
const pipeline = (config, SP_SCOPE, SP_ID, TEAM_SCOPE, TEAM_ID, CLUB_SCOPE, CLUB_ID, NATION_SCOPE, NATION_ID) => [
	{
		$match: {
			_externalSportsPersonId: SP_ID,
			_externalSportsPersonIdScope: SP_SCOPE,
			$or: [
				sportsPersonTeamKey(SP_SCOPE, SP_ID, TEAM_SCOPE, TEAM_ID).test,
				sportsPersonClubKey(SP_SCOPE, SP_ID, CLUB_SCOPE, CLUB_ID).test,
				sportsPersonNationKey(SP_SCOPE, SP_ID, NATION_SCOPE, NATION_ID).test,
			],
		},
	},
	{
		$facet: {
			meta: staffMetaFacet,
			teams: staffTeamsFacet,
			clubs: staffClubsFacet,
			nations: staffNationsFacet,
			sportsPersons: staffSportsPersonsFacet,
		},
	},
	{
		$project: {
			resourceType: { $first: '$meta.resourceType' },
			externalKey: {
				$cond: [
					sportsPersonTeamKey(SP_SCOPE, SP_ID, TEAM_SCOPE, TEAM_ID).testWithMeta,
					sportsPersonTeamKey(SP_SCOPE, SP_ID, TEAM_SCOPE, TEAM_ID).expressionWithMeta,
					{
						$cond: [
							sportsPersonClubKey(SP_SCOPE, SP_ID, CLUB_SCOPE, CLUB_ID).testWithMeta,
							sportsPersonClubKey(SP_SCOPE, SP_ID, CLUB_SCOPE, CLUB_ID).expressionWithMeta,
							{
								$cond: [
									sportsPersonNationKey(SP_SCOPE, SP_ID, NATION_SCOPE, NATION_ID).testWithMeta,
									sportsPersonNationKey(SP_SCOPE, SP_ID, NATION_SCOPE, NATION_ID).expressionWithMeta,
									null,
								],
							},
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
			teams: { $ifNull: [{ $first: '$teams.ids' }, []] },
			teamKeys: { $ifNull: [{ $first: '$teams.keys' }, []] },
			clubs: { $ifNull: [{ $first: '$clubs.ids' }, []] },
			clubKeys: { $ifNull: [{ $first: '$clubs.keys' }, []] },
			nations: { $ifNull: [{ $first: '$nations.ids' }, []] },
			nationKeys: { $ifNull: [{ $first: '$nations.keys' }, []] },
			sportsPersons: { $ifNull: [{ $first: '$sportsPersons.ids' }, []] },
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
function queryForStaffAggregationDoc(sportsPersonId, sportsPersonIdScope, teamId, teamIdScope, clubId, clubIdScope, nationId, nationIdScope) {
	if (teamId && teamIdScope) {
		return {
			resourceType: 'staff',
			externalKey: sportsPersonTeamKey(sportsPersonIdScope, sportsPersonId, teamIdScope, teamId).key,
		};
	}
	if (clubId && clubIdScope) {
		return {
			resourceType: 'staff',
			externalKey: sportsPersonClubKey(sportsPersonIdScope, sportsPersonId, clubIdScope, clubId).key,
		};
	}
	if (nationId && nationIdScope) {
		return {
			resourceType: 'staff',
			externalKey: sportsPersonNationKey(sportsPersonIdScope, sportsPersonId, nationIdScope, nationId).key,
		};
	}
	return null;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Generates a sports person team key object with key generation, test conditions, and MongoDB aggregation expressions.
 *
 * @param {string} SP_SCOPE - The scope identifier for the sports person
 * @param {string} SP_ID - The unique identifier for the sports person
 * @param {string} TEAM_SCOPE - The scope identifier for the team
 * @param {string} TEAM_ID - The unique identifier for the team
 * @returns {Object} An object containing:
 *   - key: A concatenated string key using the provided parameters
 *   - test: MongoDB query object for matching documents with team associations only
 *   - testWithMeta: MongoDB aggregation expression for testing meta field conditions
 *   - expression: MongoDB aggregation expression for constructing the key from meta fields
 *
 * @description This function creates a composite key and associated MongoDB operations
 * for identifying sports person-team relationships while excluding club and nation associations.
 * The returned object is typically used in MongoDB aggregation pipelines for grouping and filtering.
 */
const sportsPersonTeamKey = (SP_SCOPE, SP_ID, TEAM_SCOPE, TEAM_ID) => {
	return {
		key: `${SP_ID}${keySeparator}${SP_SCOPE}${teamSeparator}${TEAM_ID}${keySeparator}${TEAM_SCOPE}`,
		test: {
			$and: [
				{ _externalTeamId: TEAM_ID },
				{ _externalTeamIdScope: TEAM_SCOPE },
				{ _externalClubId: { $in: [null, undefined] } },
				{ _externalClubIdScope: { $in: [null, undefined] } },
				{ _externalNationId: { $in: [null, undefined] } },
				{ _externalNationIdScope: { $in: [null, undefined] } },
			],
		},
		testWithMeta: {
			$and: [
				{ $ne: [{ $first: '$meta._externalTeamId' }, null] },
				{ $ne: [{ $first: '$meta._externalTeamIdScope' }, null] },
				{ $eq: [{ $first: '$meta._externalClubId' }, null] },
				{ $eq: [{ $first: '$meta._externalClubIdScope' }, null] },
				{ $eq: [{ $first: '$meta._externalNationId' }, null] },
				{ $eq: [{ $first: '$meta._externalNationIdScope' }, null] },
			],
		},
		expressionWithMeta: {
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
	};
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Creates a sports person-club key configuration object for MongoDB aggregation pipelines.
 * This function generates matching criteria and key expressions for identifying relationships
 * between sports persons and clubs, excluding team and nation associations.
 *
 * @param {string} SP_SCOPE - The scope/namespace for the sports person ID
 * @param {string} SP_ID - The external sports person identifier
 * @param {string} CLUB_SCOPE - The scope/namespace for the club ID
 * @param {string} CLUB_ID - The external club identifier
 * @returns {Object} Configuration object containing:
 *   - key: String representation of the sports person-club relationship
 *   - test: MongoDB query object for matching documents without meta fields
 *   - testWithMeta: MongoDB aggregation expression for matching documents with meta fields
 *   - expression: MongoDB aggregation expression for generating the key from meta fields
 *
 * @example
 * const config = sportsPersonClubKey('FIFA', 'player123', 'UEFA', 'club456');
 * // Returns object with key matching criteria for player-club relationships
 */
const sportsPersonClubKey = (SP_SCOPE, SP_ID, CLUB_SCOPE, CLUB_ID) => {
	return {
		key: `${SP_ID}${keySeparator}${SP_SCOPE}${clubSeparator}${CLUB_ID}${keySeparator}${CLUB_SCOPE}`,
		test: {
			$and: [
				{ _externalClubId: CLUB_ID },
				{ _externalClubIdScope: CLUB_SCOPE },
				{ _externalTeamId: { $in: [null, undefined] } },
				{ _externalTeamIdScope: { $in: [null, undefined] } },
				{ _externalNationId: { $in: [null, undefined] } },
				{ _externalNationIdScope: { $in: [null, undefined] } },
			],
		},
		testWithMeta: {
			$and: [
				{ $eq: [{ $first: '$meta._externalTeamId' }, null] },
				{ $eq: [{ $first: '$meta._externalTeamIdScope' }, null] },
				{ $ne: [{ $first: '$meta._externalClubId' }, null] },
				{ $ne: [{ $first: '$meta._externalClubIdScope' }, null] },
				{ $eq: [{ $first: '$meta._externalNationId' }, null] },
				{ $eq: [{ $first: '$meta._externalNationIdScope' }, null] },
			],
		},
		expressionWithMeta: {
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
	};
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Creates a sports person nation key configuration object for MongoDB aggregation pipelines.
 * This function generates a unique key structure to identify a sports person by their nation affiliation,
 * excluding team and club associations.
 *
 * @param {string} SP_SCOPE - The scope/source identifier for the sports person
 * @param {string} SP_ID - The external ID of the sports person
 * @param {string} NATION_SCOPE - The scope/source identifier for the nation
 * @param {string} NATION_ID - The external ID of the nation
 * @returns {Object} Configuration object containing:
 *   - key: Concatenated string identifier for the sports person-nation relationship
 *   - test: MongoDB query object to match documents with nation but no team/club
 *   - testWithMeta: Aggregation expression to validate meta fields for nation-only associations
 *   - expression: MongoDB aggregation expression to construct the key from meta fields
 * @example
 * const config = sportsPersonNationKey('FIFA', '12345', 'FIFA', 'USA');
 * // Returns object with key: 'FIFA|12345::FIFA|USA' and corresponding test/expression objects
 */
const sportsPersonNationKey = (SP_SCOPE, SP_ID, NATION_SCOPE, NATION_ID) => {
	return {
		key: `${SP_ID}${keySeparator}${SP_SCOPE}${nationSeparator}${NATION_ID}${keySeparator}${NATION_SCOPE}`,
		test: {
			$and: [
				{ _externalNationId: NATION_ID },
				{ _externalNationIdScope: NATION_SCOPE },
				{ _externalTeamId: { $in: [null, undefined] } },
				{ _externalTeamIdScope: { $in: [null, undefined] } },
				{ _externalClubId: { $in: [null, undefined] } },
				{ _externalClubIdScope: { $in: [null, undefined] } },
			],
		},
		testWithMeta: {
			$and: [
				{ $eq: [{ $first: '$meta._externalTeamId' }, null] },
				{ $eq: [{ $first: '$meta._externalTeamIdScope' }, null] },
				{ $eq: [{ $first: '$meta._externalClubId' }, null] },
				{ $eq: [{ $first: '$meta._externalClubIdScope' }, null] },
				{ $ne: [{ $first: '$meta._externalNationId' }, null] },
				{ $ne: [{ $first: '$meta._externalNationIdScope' }, null] },
			],
		},
		expressionWithMeta: {
			$concat: [
				{ $first: '$meta._externalSportsPersonId' },
				keySeparator,
				{ $first: '$meta._externalSportsPersonIdScope' },
				nationSeparator,
				{ $first: '$meta._externalNationId' },
				keySeparator,
				{ $first: '$meta._externalNationIdScope' },
			],
		},
	};
};

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
module.exports = { pipeline, queryForStaffAggregationDoc, sportsPersonTeamKey, sportsPersonClubKey, sportsPersonNationKey };
