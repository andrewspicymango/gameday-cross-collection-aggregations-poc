////////////////////////////////////////////////////////////////////////////////
// Builds or rebuilds nation aggregation documents and updates inbound references
// (teams, venues, sgos) to maintain bidirectional consistency.
////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline, queryForStaffAggregationDoc } = require('./staffAggregationPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
async function processStaff(config, mongo, sportsPersonId, sportsPersonIdScope, teamId, teamIdScope, clubId, clubIdScope, requestId) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!sportsPersonId || !sportsPersonIdScope) throw new Error('Invalid parameters: sportsPersonId and sportsPersonIdScope are required');
	if (!((_.isString(teamId) && _.isString(teamIdScope)) || (_.isString(clubId) && _.isString(clubIdScope))))
		throw new Error('Invalid parameters: either teamId and teamIdScope or clubId and clubIdScope are required');

	//////////////////////////////////////////////////////////////////////////////
	debug(
		`sportsPersonIdScope=${sportsPersonIdScope}, sportsPersonId=${sportsPersonId}, teamIdScope=${teamIdScope}, teamId=${teamId}, clubIdScope=${clubIdScope}, clubId=${clubId}`,
		requestId
	);

	//////////////////////////////////////////////////////////////////////////////
	// Validate staff member exists
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
	} else if (clubId && clubIdScope) {
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
	const pipelineObj = pipeline(config, sportsPersonIdScope, sportsPersonId, teamIdScope, teamId, clubIdScope, clubId);
	const staffAggQuery = queryForStaffAggregationDoc(sportsPersonId, sportsPersonIdScope, teamId, teamIdScope, clubId, clubIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Get the original aggregation document (if it exists) and calculate old outbound keys
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(staffAggQuery);
	//////////////////////////////////////////////////////////////////////////////
	// Run the pipeline to build the aggregation document
	await runPipeline(mongo, 'staff', pipelineObj, requestId);
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(staffAggQuery);
	//////////////////////////////////////////////////////////////////////////////
	const operations = buildOperationsForReferenceChange(oldAggregationDoc, newAggregationDoc);
	await executeOperationsForReferenceChange(mongo, config, operations, requestId);
	//////////////////////////////////////////////////////////////////////////////
	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processStaff };
