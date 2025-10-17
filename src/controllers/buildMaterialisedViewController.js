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
const { processClub } = require('../pipelines/club/clubAggregationBuild.js');
const { processNation } = require('../pipelines/nation/nationAggregationBuild.js');
const { processStaff } = require('../pipelines/staff/staffAggregationBuild.js');
const { processVenue } = require('../pipelines/venue/venueAggregationBuild.js');
const { processKeyMoment } = require('../pipelines/keyMoment/keyMomentAggregationBuild.js');
const { processSportsPerson } = require('../pipelines/sportsPerson/sportsPersonAggregationBuild.js');

////////////////////////////////////////////////////////////////////////////////
// Constants

////////////////////////////////////////////////////////////////////////////////
// Notes
// curl -X POST localhost:8080/1-0/aggregate/competitions/bblapi/2023:BBL
// curl -X POST localhost:8080/1-0/aggregate/stages/bblapi/2023:BBL:league
// curl -X POST localhost:8080/1-0/aggregate/events/bblapi/6ea116b7-7c38-47b3-a1ee-db90108034b2
// curl -X POST localhost:8080/1-0/aggregate/teams/bblapi/9a259543-a5a3-4f51-b352-9ceeffb4ae15
// curl -X POST localhost:8080/1-0/aggregate/events/bblscb/2003994
// curl -X POST localhost:8080/1-0/aggregate/km/bblscb/2003994/urn:gd:km:type:action/urn:gd:km:subtype:startMatch/2025-10-03T15:50:06Z
// curl -X POST localhost:8080/1-0/aggregate/km/bblscb/2003994/urn:gd:km:type:action/urn:gd:km:subtype:confirmTeam/2025-10-03T15:50:06Z
// curl -X POST localhost:8080/1-0/aggregate/staff/sp/bblscb/56414/team/bblscb/413
// curl -X POST localhost:8080/1-0/aggregate/sportspersons/bblscb/56414

//
// curl -X POST localhost:8080/1-0/aggregate/competitions/fifa/289175
// curl -X POST localhost:8080/1-0/aggregate/events/fifa/146186
// curl -X POST localhost:8080/1-0/aggregate/sgos/fifa/confederation_23914
// curl -X POST localhost:8080/1-0/aggregate/clubs/fifa/1950154
// curl -X POST localhost:8080/1-0/aggregate/stages/fifa/289179
// curl -X POST localhost:8080/1-0/aggregate/nations/fifa/srb
// curl -X POST localhost:8080/1-0/aggregate/staff/sp/fifa/394503/team/fifa/289175_1884422
// curl -X POST localhost:8080/1-0/aggregate/teams/fifa/289175_1954283
// curl -X POST localhost:8080/1-0/aggregate/sgos/fifa/confederation_0
// curl -X POST localhost:8080/1-0/aggregate/sgos/fifa/confederation_23915
// curl -X POST localhost:8080/1-0/aggregate/sgos/fifa/association_21914
// curl -X POST localhost:8080/1-0/aggregate/venues/fifa/5000247

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
async function buildMaterialisedViewControllerForIdScopeResources(req, res) {
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
	//////////////////////////////////////////////////////////////////////////////
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
		// CLUBS
		else if (schemaType.toLowerCase() == 'clubs') {
			response = await processClub(config, mongo, scope, requestedId, id);
		}
		////////////////////////////////////////////////////////////////////////////
		// NATIONS
		else if (schemaType.toLowerCase() == 'nations') {
			response = await processNation(config, mongo, scope, requestedId, id);
		}
		////////////////////////////////////////////////////////////////////////////
		// VENUES
		else if (schemaType.toLowerCase() == 'venues') {
			response = await processVenue(config, mongo, scope, requestedId, id);
		}
		////////////////////////////////////////////////////////////////////////////
		// SPORTS PERSONS
		else if (schemaType.toLowerCase() == 'sportspersons') {
			response = await processSportsPerson(config, mongo, scope, requestedId, id);
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
		// If we have a 500 from the processor, return that
		if (response == null) {
			send500(res, `Failed to build aggregation document for ${schemaType} with id ${requestedId} and scope ${scope}`);
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
// router.post('/aggregate/staff/sp/:spScope/:spId/:type/:orgIdScope/:orgId', buildMaterialisedViewControllerForStaff);
async function buildMaterialisedViewControllerForStaff(req, res) {
	const id = uuid.v4();
	debug(`${req.method} ${req.url}${req.hostname != undefined ? ' [called from ' + req.hostname + ']' : ''}`, id);
	const spScope = req.params.spScope;
	const spId = req.params.spId;
	const type = req.params.type;
	const orgIdScope = req.params.orgIdScope;
	const orgId = req.params.orgId;
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
	const report = `Creating materialised aggregation view of single sports data for schemaType: staff, sports person scope: ${spScope}, sports person id: ${spId}, ${type} id scope: ${orgIdScope}, org id: ${orgId} with query strings ${JSON.stringify(
		req.query
	)}`;
	info(report, id);
	try {
		////////////////////////////////////////////////////////////////////////////
		// Build the materialised aggregation view for team staff
		if (type.toLowerCase() === 'team') {
			const response = await processStaff(config, mongo, spId, spScope, orgId, orgIdScope, null, null, null, null, id);
			if (response == null) {
				send500(res, 'Failed to build aggregation document for staff resource');
				return;
			}
			////////////////////////////////////////////////////////////////////////////
			// Return the result
			const body = {
				status: 200,
				service: config?.serviceName,
				message: `Materialised aggregation views created for staff resource SP ${spScope}/${spId} and Team ${orgIdScope}/${orgId}`,
				response,
			};
			send200(res, body, config);
			return;
		}
		////////////////////////////////////////////////////////////////////////////
		// Build the materialised aggregation view for club staff
		else if (type.toLowerCase() === 'club') {
			const response = await processStaff(config, mongo, spId, spScope, null, null, orgId, orgIdScope, null, null, id);
			////////////////////////////////////////////////////////////////////////////
			// Return the result
			const body = {
				status: 200,
				service: config?.serviceName,
				message: `Materialised aggregation views created for staff resource SP ${spScope}/${spId} and Club ${orgIdScope}/${orgId}`,
				response,
			};
			send200(res, body, config);
			return;
		}
		/////////////////////////////////////////////////////////////////////////
		else if (type.toLowerCase() === 'nation') {
			const response = await processStaff(config, mongo, spId, spScope, null, null, null, null, orgId, orgIdScope, id);
			////////////////////////////////////////////////////////////////////////////
			// Return the result
			const body = {
				status: 200,
				service: config?.serviceName,
				message: `Materialised aggregation views created for staff resource SP ${spScope}/${spId} and Nation ${orgIdScope}/${orgId}`,
				response,
			};
			send200(res, body, config);
			return;
		}
	} catch (err) {
		send500(res, err.message);
		return;
	}
}

///////////////////////////////////////////////////////////////////////////////
// router.post('/aggregate/km/:eventIdScope/:eventId/:type/:subType/:dateTime', buildMaterialisedViewControllerForKeyMoment);
async function buildMaterialisedViewControllerForKeyMoment(req, res) {
	const id = uuid.v4();
	debug(`${req.method} ${req.url}${req.hostname != undefined ? ' [called from ' + req.hostname + ']' : ''}`, id);
	const eventIdScope = req.params.eventIdScope;
	const eventId = req.params.eventId;
	const type = req.params.type;
	const subType = req.params.subType;
	const dateTime = req.params.dateTime;
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
	const report = `Creating materialised aggregation view of single sports data for schemaType: keyMoment, event id scope: ${eventIdScope}, event id: ${eventId}, type: ${type}, sub type: ${subType}, date time: ${dateTime} with query strings ${JSON.stringify(
		req.query
	)}`;
	info(report, id);
	try {
		const response = await processKeyMoment(config, mongo, eventIdScope, eventId, type, subType, dateTime, id);
		////////////////////////////////////////////////////////////////////////////
		// Return the result
		const body = {
			status: 200,
			service: config?.serviceName,
			message: `Materialised aggregation views created for keyMoment resource ${eventIdScope}/${eventId} and type ${type} and sub type ${subType} and date time ${dateTime}`,
			response,
		};
		send200(res, body, config);
	} catch (err) {
		send500(res, err.message);
		return;
	}
}

////////////////////////////////////////////////////////////////////////////////
module.exports = {
	buildMaterialisedViewControllerForIdScopeResources,
	buildMaterialisedViewControllerForStaff,
	buildMaterialisedViewControllerForKeyMoment,
};
