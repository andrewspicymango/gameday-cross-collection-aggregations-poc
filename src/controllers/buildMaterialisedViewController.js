////////////////////////////////////////////////////////////////////////////////
// Includes
const _ = require(`lodash`);
const uuid = require('uuid');
const { send200, send400, send404, send500 } = require('../utils/httpResponseUtils.js');
const { debug, info, warn } = require('../log.js');
const config = require('../config.js');
const runPipeline = require('../pipelines/runPipeline.js');

////////////////////////////////////////////////////////////////////////////////
const { processCompetition } = require('../pipelines/competition/competitionAggregationBuild.js');
const { processStage } = require('../pipelines/stage/stageAggregationBuild.js');
const { processEvent } = require('../pipelines/event/eventAggregationBuild.js');
const { processTeam } = require('../pipelines/team/teamAggregationBuild.js');
const { processSgo } = require('../pipelines/sgo/sgoAggregationBuild.js');

////////////////////////////////////////////////////////////////////////////////
// Constants

////////////////////////////////////////////////////////////////////////////////
// Notes
// curl -X POST localhost:8080/1-0/aggregate/competitions/bblapi/2023:BBL
// curl -X POST localhost:8080/1-0/aggregate/stages/bblapi/2023:BBL:league
// curl -X POST localhost:8080/1-0/aggregate/events/bblapi/6ea116b7-7c38-47b3-a1ee-db90108034b2
// curl -X POST localhost:8080/1-0/aggregate/teams/bblapi/9a259543-a5a3-4f51-b352-9ceeffb4ae15
//
// curl -X POST localhost:8080/1-0/aggregate/competitions/fifa/289175
// curl -X POST localhost:8080/1-0/aggregate/events/fifa/146186
// curl -X POST localhost:8080/1-0/aggregate/sgos/fifa/confederation_23914

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

	//////////////////////////////////////////////////////////////////////////////
	// Build the materialised aggregation view
	try {
		let response;
		//////////////////////////////////////////////////////////////////////////////
		// Route the request to the appropriate processor based on the schema type
		////////////////////////////////////////////////////////////////////////////
		// COMPETITIONS
		if (schemaType.toLowerCase() == 'competitions') {
			response = await processCompetition(config, mongo, scope, requestedId, id);
		}
		////////////////////////////////////////////////////////////////////////////
		// STAGES
		else if (schemaType.toLowerCase() == 'stages') {
			response = await processStage(config, mongo, scope, requestedId, id);
		}
		////////////////////////////////////////////////////////////////////////////
		// EVENTS
		else if (schemaType.toLowerCase() == 'events') {
			response = await processEvent(config, mongo, scope, requestedId, id);
		}
		////////////////////////////////////////////////////////////////////////////
		// TEAMS
		else if (schemaType.toLowerCase() == 'teams') {
			response = await processTeam(config, mongo, scope, requestedId, id);
		}
		//////////////////////////////////////////////////////////////////////////
		// SGOs
		else if (schemaType.toLowerCase() == 'sgos') {
			response = await processSgo(config, mongo, scope, requestedId, id);
		}
		////////////////////////////////////////////////////////////////////////////
		// ALL OTHERS NOT YET SUPPORTED
		else {
			warn(`No valid Schema found when trying to get: ${schemaType}`, 'WD0040', 400, 'Invalid Schema');
			send400(res, {
				message: 'Please specify a valid schema type.',
				errorCode: 'WD0040',
				category: 'Invalid Schema',
			});
			return;
		}
		////////////////////////////////////////////////////////////////////////////
		// If we have a 404 from the processor, return that
		if (response === 404) {
			debug(`404: No ${schemaType} found for id ${requestedId} and scope ${scope}`, id);
			send404(res, `No ${schemaType} found for id ${requestedId} and scope ${scope}`);
			return;
		}

		////////////////////////////////////////////////////////////////////////////
		// Return the result
		const body = {
			status: 200,
			service: config?.serviceName,
			message: `Materialised aggregation views created for ${createResource ? 'new' : 'existing'} ${schemaType} ${scope}/${requestedId}`,
			response,
		};
		send200(res, body, config);
	} catch (err) {
		send500(res, err.message);
		return;
	}
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { buildMaterialisedViewController };
