////////////////////////////////////////////////////////////////////////////////
// Builds or rebuilds nation aggregation documents and updates inbound references
// (teams, venues, sgos) to maintain bidirectional consistency.
////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug, warn } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline, queryForStaffAggregationDoc } = require('./staffAggregationPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
/**
 * Processes staff aggregation for a sports person associated with a team, club, or nation.
 * Validates the staff member exists, builds aggregation pipeline, and updates references.
 *
 * @async
 * @function processStaff
 * @param {Object} config - Configuration object containing mongo settings
 * @param {string} config.mongo.matAggCollectionName - Name of the materialized aggregation collection
 * @param {Object} mongo - MongoDB connection object with db property
 * @param {string} sportsPersonId - External sports person identifier
 * @param {string} sportsPersonIdScope - Scope/namespace for the sports person ID
 * @param {string} [teamId] - External team identifier (required if team scope provided)
 * @param {string} [teamIdScope] - Scope/namespace for the team ID
 * @param {string} [clubId] - External club identifier (required if club scope provided)
 * @param {string} [clubIdScope] - Scope/namespace for the club ID
 * @param {string} [nationId] - External nation identifier (required if nation scope provided)
 * @param {string} [nationIdScope] - Scope/namespace for the nation ID
 * @param {string} requestId - Unique identifier for the request (used for logging)
 * @param {boolean} [updatedReferences=true] - Whether to update cross-references after aggregation
 * @returns {Promise<Object|number|null>} Returns 404 if staff not found, null on failure, or the new aggregation document
 * @throws {Error} Throws error for invalid configuration or missing required parameters
 *
 * @description
 * - Validates that either team, club, or nation parameters are provided
 * - Checks if the staff member exists in the database
 * - Builds and executes aggregation pipeline
 * - Compares old vs new aggregation documents
 * - Updates cross-references if requested and successful
 */
async function processStaff(
	config,
	mongo,
	sportsPersonId,
	sportsPersonIdScope,
	teamId,
	teamIdScope,
	clubId,
	clubIdScope,
	nationId,
	nationIdScope,
	requestId,
	updatedReferences = true
) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!sportsPersonId || !sportsPersonIdScope) throw new Error('Invalid parameters: sportsPersonId and sportsPersonIdScope are required');
	if (!((_.isString(teamId) && _.isString(teamIdScope)) || (_.isString(clubId) && _.isString(clubIdScope)) || (_.isString(nationId) && _.isString(nationIdScope))))
		throw new Error('Invalid parameters: either teamId and teamIdScope or clubId and clubIdScope or nationId and nationIdScope are required');

	//////////////////////////////////////////////////////////////////////////////
	debug(
		`sportsPersonIdScope=${sportsPersonIdScope}, sportsPersonId=${sportsPersonId}, teamIdScope=${teamIdScope}, teamId=${teamId}, clubIdScope=${clubIdScope}, clubId=${clubId}, nationIdScope=${nationIdScope}, nationId=${nationId}`,
		requestId
	);

	//////////////////////////////////////////////////////////////////////////////
	// Validate team staff member exists
	if (teamId && teamIdScope) {
		const exists = await mongo.db
			.collection('staff')
			.countDocuments(
				{ _externalSportsPersonId: sportsPersonId, _externalSportsPersonIdScope: sportsPersonIdScope, _externalTeamId: teamId, _externalTeamIdScope: teamIdScope },
				{ limit: 1 }
			);
		if (exists === 0) {
			debug(`Staff member not found: [sp] ${sportsPersonId}@${sportsPersonIdScope}, [t] ${teamId}@${teamIdScope}`, requestId);
			return 404;
		}
	}
	//////////////////////////////////////////////////////////////////////////////
	// Validate club staff member exists
	else if (clubId && clubIdScope) {
		const exists = await mongo.db
			.collection('staff')
			.countDocuments(
				{ _externalSportsPersonId: sportsPersonId, _externalSportsPersonIdScope: sportsPersonIdScope, _externalClubId: clubId, _externalClubIdScope: clubIdScope },
				{ limit: 1 }
			);
		if (exists === 0) {
			debug(`Staff member not found: [sp] ${sportsPersonId}@${sportsPersonIdScope}, [c] ${clubId}@${clubIdScope}`, requestId);
			return 404;
		}
	}
	//////////////////////////////////////////////////////////////////////////////
	// Validate nation staff member exists
	else if (nationId && nationIdScope) {
		const exists = await mongo.db
			.collection('staff')
			.countDocuments(
				{ _externalSportsPersonId: sportsPersonId, _externalSportsPersonIdScope: sportsPersonIdScope, _externalNationId: nationId, _externalNationIdScope: nationIdScope },
				{ limit: 1 }
			);
		if (exists === 0) {
			debug(`Staff member not found: [sp] ${sportsPersonId}@${sportsPersonIdScope}, [n] ${nationId}@${nationIdScope}`, requestId);
			return 404;
		}
	}

	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, sportsPersonIdScope, sportsPersonId, teamIdScope, teamId, clubIdScope, clubId, nationIdScope, nationId);
	const staffAggQuery = queryForStaffAggregationDoc(sportsPersonId, sportsPersonIdScope, teamId, teamIdScope, clubId, clubIdScope, nationId, nationIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Get the original aggregation document (if it exists) and calculate old outbound keys
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(staffAggQuery);
	//////////////////////////////////////////////////////////////////////////////
	// Run the pipeline to build the aggregation document
	await runPipeline(mongo, 'staff', pipelineObj, requestId);
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(staffAggQuery);
	//////////////////////////////////////////////////////////////////////////////
	if (!_.isObject(newAggregationDoc)) {
		warn(`Failed to build new aggregation document`, requestId);
		return null;
	}
	//////////////////////////////////////////////////////////////////////////////
	// Compare old and new aggregation documents to determine if references need to be updated
	if (updatedReferences === true) {
		const operations = buildOperationsForReferenceChange(oldAggregationDoc, newAggregationDoc);
		await executeOperationsForReferenceChange(mongo, config, operations, requestId);
	}
	//////////////////////////////////////////////////////////////////////////////
	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processStaff };
