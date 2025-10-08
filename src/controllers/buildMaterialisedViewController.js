////////////////////////////////////////////////////////////////////////////////
// Includes
const _ = require(`lodash`);
const uuid = require('uuid');
const { send200, send400, send404, send500 } = require('../utils/httpResponseUtils.js');
const { debug, info, warn } = require('../log.js');
const config = require('../config.js');
const competitionFullPipeline = require('../pipelines/competition-sgo-stage-event-venue-team-sportsperson.js');
const runPipeline = require('../pipelines/runPipeline.js');

////////////////////////////////////////////////////////////////////////////////
// Constants
const matAggCollectionName = `materialisedAggregations`;

////////////////////////////////////////////////////////////////////////////////
// Notes
// curl -X POST localhost:8080/1-0/aggregate/competitions/bblapi/2023:BBL?create=true
// curl -X POST localhost:8080/1-0/aggregate/competitions/fifa/1jt5mxgn4q5r6mknmlqv5qjh0?create=true

////////////////////////////////////////////////////////////////////////////////
/**
 * Map a provided schema name (case-insensitive) to the internal collection key.
 *
 * This helper normalizes the input schema name and returns the corresponding
 * collection identifier used by the application. If the input is falsy
 * (null/undefined/empty string) or does not match any known schema, the
 * function returns null.
 *
 * Recognized mappings:
 *  - "competitions"  => "competitions"
 *  - "clubs"         => "clubs"
 *  - "rankings"      => "rankings"
 *  - "keymoments"    => "keyMoments"
 *  - "sportspersons" => "sportsPersons"
 *  - "stages"        => "stages"
 *  - "teams"         => "teams"
 *  - "venues"        => "venues"
 *  - "events"        => "events"
 *  - "stickies"      => "stickies"
 *  - "relationships" => "relationships"
 *  - "staff"         => "staff"
 *  - "stories"       => "stories"
 *  - "nations"       => "nations"
 *  - "sgos"          => "sgos"
 *
 * @function checkSchema
 * @param {string|null|undefined} schema - The input schema name to normalize and match. Comparison is case-insensitive.
 * @returns {string|null} The normalized collection name if recognized; otherwise null.
 * @example
 * checkSchema('Clubs'); // => 'clubs'
 * @example
 * checkSchema('KeyMoments'); // => 'keyMoments'
 * @example
 * checkSchema('unknown'); // => null
 */
const checkSchema = function (schema) {
	if (!schema) return null;
	switch (schema.toLowerCase()) {
		case 'competitions':
			return 'competitions';
		case 'clubs':
			return 'clubs';
		case 'rankings':
			return 'rankings';
		case 'keymoments':
			return 'keyMoments';
		case 'sportspersons':
			return 'sportsPersons';
		case 'stages':
			return 'stages';
		case 'teams':
			return 'teams';
		case 'venues':
			return 'venues';
		case 'events':
			return 'events';
		case 'stickies':
			return 'stickies';
		case 'relationships':
			return 'relationships';
		case 'staff':
			return 'staff';
		case 'stories':
			return 'stories';
		case 'nations':
			return 'nations';
		case 'sgos':
			return 'sgos';
		default:
			return null;
	}
};

////////////////////////////////////////////////////////////////////////////////
async function buildMaterialisedViewController(req, res) {
	const id = uuid.v4();
	debug(`${req.method} ${req.url}${req.hostname != undefined ? ' [called from ' + req.hostname + ']' : ''}`, id);
	const schemaType = req.params.schemaType;
	const scope = req.params.scope;
	const requestedId = req.params.id;
	const createResource = req?.query?.create && req.query.create === 'true' ? true : false;
	const schema = checkSchema(schemaType);
	const mongo = config?.mongo;

	//////////////////////////////////////////////////////////////////////////////
	if (!mongo || !mongo.db || !mongo.client) {
		warn(`No MongoDB connection available`, 'WD0050', 500, 'Database Connection Error');
		send400(res, {
			message: 'Database connection is not available.',
			errorCode: 'WD0050', // TODO: Error codes should be documented in a central location and not as magic numbers in code
			category: 'Database Connection Error',
		});
		return;
	}

	//////////////////////////////////////////////////////////////////////////////
	if (!schema) {
		warn(`No valid Schema found when trying to get: ${schemaType}`, 'WD0040', 400, 'Invalid Schema');
		send400(res, {
			message: 'Please specify a valid schema type.',
			errorCode: 'WD0040', // TODO: Error codes should be documented in a central location and not as magic numbers in code
			category: 'Invalid Schema',
		});
		return;
	}

	const report = `Creating materialised aggregation view of single sports data for schemaType: ${schemaType}, scope: ${scope}, id: ${requestedId} with query strings ${JSON.stringify(
		req.query
	)}`;
	info(report, id);

	try {
		switch (schemaType.toLowerCase()) {
			case 'competitions':
				if (createResource) await _getMongoCompetitionAggregatedView(mongo, scope, requestedId, id);
				// if (createResource) await _getMongoCompetitionStageAggregatedView(mongo, scope, requestedId, id);
				// if (createResource) await _getMongoCompetitionEventVenueAggregatedView(mongo, scope, requestedId, id);

				send200(res, { status: 200, service: config?.serviceName, message: `Materialised aggregation view created for ${schemaType} ${scope}/${requestedId}` }, config);
				return;
			case 'clubs':
			case 'rankings':
			case 'keymoments':
			case 'sportspersons':
			case 'stages':
			case 'teams':
			case 'venues':
			case 'events':
			case 'stickies':
			case 'relationships':
			case 'staff':
			case 'stories':
			case 'nations':
			case 'sgos':
				break;
			default:
				warn(`No valid Schema found when trying to get: ${schemaType}`, 'WD0040', 400, 'Invalid Schema');
				send400(res, {
					message: 'Please specify a valid schema type.',
					errorCode: 'WD0040', // TODO: Error codes should be documented in a central location and not as magic numbers in code
					category: 'Invalid Schema',
				});
				return;
		}
	} catch (err) {
		send500(res, err.message);
		return;
	}
	//////////////////////////////////////////////////////////////////////////////
}
////////////////////////////////////////////////////////////////////////////////
/**
 * Build and execute an aggregation pipeline to materialize a competition view in MongoDB.
 *
 * This async helper constructs a full aggregation pipeline for a given competition scope and id
 * (using `competitionFullPipeline`) and executes it against the "competitions" collection by
 * delegating to `runPipeline`. If either `competitionIdScope` or `competitionId` are falsy, the
 * function short-circuits and returns null.
 *
 * @async
 * @private
 * @param {Object} mongo - MongoDB connection/context object used by `runPipeline` (e.g. a Db or client wrapper).
 * @param {string} competitionIdScope - Scope/type/namespace for the competition id (must be truthy).
 * @param {(string|number)} competitionId - Identifier of the competition to build the aggregated view for (must be truthy).
 * @param {string} [requestId] - Optional request identifier used for tracing/logging in `runPipeline`.
 * @returns {Promise<null|void>} Resolves to null when input validation fails (missing scope or id). Otherwise resolves
 * to void after the pipeline has been executed. Any result returned by `runPipeline` is not propagated.
 *
 * @throws {Error} Propagates errors thrown by `competitionFullPipeline` or `runPipeline`, e.g. pipeline construction
 * or MongoDB execution errors.
 *
 * @example
 * // Build and run the aggregated view for competition "123" in scope "national"
 * await _getMongoCompetitionAggregatedView(mongoDb, 'national', '123', 'req-456');
 */
async function _getMongoCompetitionAggregatedView(mongo, competitionIdScope, competitionId, requestId) {
	if (!competitionId || !competitionIdScope) return null;
	const pipeline = competitionFullPipeline(competitionIdScope, competitionId);
	await runPipeline(mongo, 'competitions', pipeline, requestId);
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { buildMaterialisedViewController };
