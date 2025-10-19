const { sportsPersonClubMemberFacet } = require('./sportsPersonClubMemberFacet');
const { sportsPersonTeamMemberFacet } = require('./sportsPersonTeamMemberFacet');
const { sportsPersonEventFacet } = require('./sportsPersonEventFacet');
const { sportsPersonKeyMomentFacet } = require('./sportsPersonKeyMomentFacet');
const { sportsPersonStaffFacet } = require('./sportsPersonStaffFacet');
const { sportsPersonMetaFacet } = require('./sportsPersonMetaFacet');
const { sportsPersonRankingsFacet } = require('./sportsPersonRankingsFacet');
const { keySeparator } = require('../constants');
const { keyInAggregation } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Creates a MongoDB aggregation pipeline for sports person data aggregation.
 *
 * The pipeline performs the following operations:
 * 1. Matches documents by external ID and scope
 * 2. Uses $facet to run parallel aggregations for meta data, clubs, teams, events, staff, and rankings
 * 3. Projects and normalizes the facet results into a structured format
 * 4. Adds computed fields including current timestamp
 * 5. Merges results into a materialized aggregations collection
 *
 * @param {Object} config - Configuration object containing MongoDB settings
 * @param {string} config.mongo.matAggCollectionName - Target collection name for materialized aggregations
 * @param {string} SP_SCOPE - External ID scope for the sports person
 * @param {string} SP_ID - External ID of the sports person to aggregate
 * @returns {Array} MongoDB aggregation pipeline array
 *
 * @example
 * const aggPipeline = pipeline(config, 'ESPN', 'player123');
 * db.collection.aggregate(aggPipeline);
 */
