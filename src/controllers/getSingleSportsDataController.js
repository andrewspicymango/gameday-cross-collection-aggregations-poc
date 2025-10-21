const _ = require(`lodash`);
const uuid = require('uuid');
const { keySeparator } = require('../pipelines/constants');
const { send200, send400, send404, send500 } = require('../utils/httpResponseUtils');
const { debug, info, warn } = require('../log.js');
const config = require('../config.js');

const { writeFileSyncWithDirs } = require(`../utils/fileUtils.js`);
const maxMaterialisedResources = 50;

// curl localhost:8080/1-0/competitions/bblapi/2023:BBL
// curl localhost:8080/1-0/competitions/fifa/289715

////////////////////////////////////////////////////////////////////////////////
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
async function getSingleSportsData(req, res) {
	const id = uuid.v4();
	debug(`${req.method} ${req.url}${req.hostname != undefined ? ' [called from ' + req.hostname + ']' : ''}`, id);
	const schemaType = req.params.schemaType;
	const scope = req.params.scope;
	const requestedId = req.params.id;
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
	const report = `Fetching single sports data for schemaType: ${schemaType}, scope: ${scope}, id: ${requestedId} with query strings ${JSON.stringify(req.query)}`;

	//////////////////////////////////////////////////////////////////////////////
	try {
		const r = await mongo.db.collection(schema).findOne(scope.toLowerCase() !== 'gameday' ? { _externalIdScope: scope, _externalId: requestedId } : { _id: requestedId });
		if (r == null) {
			warn(`No data found for schemaType: ${schemaType}, scope: ${scope}, id: ${requestedId}`, 'WD0061', 404, 'Data Not Found');
			send404(res, {
				message: 'No data found for the specified parameters.',
				errorCode: 'WDxxx', // TODO: Error codes should be documented in a central location and not as magic numbers in code
				category: 'Data Not Found',
			});
			return;
		}

		////////////////////////////////////////////////////////////////////////////
		delete r.stickies;
		if (r._original && req.query.includeOriginal !== true && req.query.includeOriginal !== 'true') delete r._original;
		if (r._stickies && req.query.includeStickies !== true && req.query.includeStickies !== 'true') delete r._stickies;

		////////////////////////////////////////////////////////////////////////////r
		info(report, id);

		////////////////////////////////////////////////////////////////////////////
		const aggregationViews = req.query?.aggregationViews ? req.query.aggregationViews : null;
		const aggregationEdges = req.query?.aggregationEdges ? req.query.aggregationEdges : null;
		let aggregationMax = req.query?.aggregationMax ? parseInt(req.query.aggregationMax, 10) : maxMaterialisedResources;
		if (isNaN(aggregationMax) || aggregationMax < 0 || aggregationMax > maxMaterialisedResources) aggregationMax = maxMaterialisedResources;
		////////////////////////////////////////////////////////////////////////////
		// We are attempting an aggregation
		if (aggregationViews && aggregationEdges && aggregationViews.length > 0 && aggregationEdges.length > 0) {
			info(`Aggregation requested: views=${aggregationViews}, edges=${aggregationEdges}, max=${aggregationMax}`, id);
			let rootKey = null;
			let rootType = null;
			const resourceType = r?.resourceType ? r.resourceType.toLowerCase() : null;
			//////////////////////////////////////////////////////////////////////////
			// determine root document type and externalKey
			switch (resourceType) {
				case 'competition':
				case 'stage':
				case 'event':
				case 'sgo':
				case 'team':
				case 'club':
				case 'nation':
				case 'venue':
					rootKey = `${r._externalId}${keySeparator}${r._externalIdScope}`;
					rootType = resourceType;
					break;
				case 'sportsperson':
					rootKey = `${r._externalId}@${r._externalIdScope}`;
					rootType = 'sportsPerson';
					break;
				case 'ranking':
					const RankingKeyClass = require('../pipelines/ranking/rankingKeyClass.js');
					const spId = r?._externalSportsPersonId ? r._externalSportsPersonId : null;
					const spIdScope = r?._externalSportsPersonIdScope ? r._externalSportsPersonIdScope : null;
					const teamId = r?._externalTeamId ? r._externalTeamId : null;
					const teamIdScope = r?._externalTeamIdScope ? r._externalTeamIdScope : null;
					const stageId = r?._externalStageId ? r._externalStageId : null;
					const stageIdScope = r?._externalStageIdScope ? r._externalStageIdScope : null;
					const eventId = r?._externalEventId ? r._externalEventId : null;
					const eventIdScope = r?._externalEventIdScope ? r._externalEventIdScope : null;
					const label = r?.dateTime ? r.dateTime : null;
					const rank = r?.ranking ? r.ranking : null;
					const keyInstance = new RankingKeyClass(spIdScope, spId, teamIdScope, teamId, stageIdScope, stageId, eventIdScope, eventId, label, ranking);
					if (!keyInstance.validate()) {
						warn(`Invalid RankingKeyClass instance for ranking id: ${requestedId}`, id);
						send400(res, {
							message: 'Invalid ranking data for aggregation.',
							errorCode: 'WDxxx', // TODO: Error codes should be documented in a central location and not as magic numbers in code
						});
						return;
					}
					const key = keyInstance.rankingDocumentKey();
					if (!key) {
						warn(`Unable to determine rootKey for ranking id: ${requestedId}`, id);
						send500(res, {
							message: 'Unable to determine root key for ranking aggregation.',
							errorCode: 'WDxxx', // TODO: Error codes should be documented in a central location and not as magic numbers in code
						});
						return;
					}
					rootType = 'ranking';
					rootKey = key;
					break;
				case 'staff':
					const { queryForStaffAggregationDoc } = require('../pipelines/staff/staffAggregationPipeline.js');
					const staffQuery = queryForStaffAggregationDoc(
						r?._externalSportsPersonId,
						r?._externalSportsPersonIdScope,
						r?._externalTeamId,
						r?._externalTeamIdScope,
						r?._externalClubId,
						r?._externalClubIdScope,
						r?._externalNationId,
						r?._externalNationIdScope
					);
					if (!staffQuery?.externalKey) {
						warn(`Unable to determine rootKey for staff id: ${requestedId}`, id);
						send500(res, {
							message: 'Unable to determine root key for staff aggregation.',
							errorCode: 'WDxxx', // TODO: Error codes should be documented in a central location and not as magic numbers in code
						});
						return;
					}
					rootType = 'staff';
					rootKey = staffQuery.externalKey;
					break;
				case 'keymoment':
					const { queryForKeyMomentAggregationDoc } = require('../pipelines/keyMoment/keyMomentAggregationPipeline.js');
					const kmQuery = queryForKeyMomentAggregationDoc(r?._externalEventId, r?._externalEventIdScope, r?.type, r?.subType, r?.dateTime);
					if (!kmQuery?.externalKey) {
						warn(`Unable to determine rootKey for keyMoment id: ${requestedId}`, id);
						send500(res, {
							message: 'Unable to determine root key for keyMoment aggregation.',
							errorCode: 'WDxxx', // TODO: Error codes should be documented in a central location and not as magic numbers in code
						});
						return;
					}
					rootType = 'keyMoment';
					rootKey = kmQuery.externalKey;
					break;
				default:
					warn(`Aggregation not supported for resourceType: ${resourceType}`, id);
					send200(res, r);
					return;
			}

			//////////////////////////////////////////////////////////////////////////
			// Manage the projections
			const fieldProjections = {};
			fieldProjections.exclusions = {};
			fieldProjections.exclusions.all = {};
			fieldProjections.inclusions = {};
			fieldProjections.inclusions.all = {};
			if (req.query.includeOriginal !== true && req.query.includeOriginal !== 'true') fieldProjections.exclusions.all._original = 0;
			if (req.query.includeStickies !== true && req.query.includeStickies !== 'true') fieldProjections.exclusions.all._stickies = 0;
			for (const [key, value] of Object.entries(req?.query || {})) {
				if (key.startsWith('projection.')) {
					const field = key.replace('projection.', '');
					fieldProjections.inclusions[field] = value.split(',').reduce((acc, curr) => {
						acc[curr.trim()] = 1;
						return acc;
					}, {});
				}
				if (key.startsWith('projection~')) {
					const field = key.replace('projection~', '');
					fieldProjections.exclusions[field] = value.split(',').reduce((acc, curr) => {
						acc[curr.trim()] = 0;
						return acc;
					}, {});
				}
			}

			//////////////////////////////////////////////////////////////////////////
			const clientAggregationPipelineRouteBuilder = require('../client/clientAggregationPipelineRouteBuilder.js');
			const clientAggregationPipelineBuilder = require('../client/clientAggregationPipelineBuilder.js');
			const routes = clientAggregationPipelineRouteBuilder({ rootType, includeTypes: aggregationViews.split(','), edgeIds: aggregationEdges.split(',') });
			const pipelineConfig = { rootType, rootExternalKey: rootKey, totalMax: aggregationMax, routes, includeTypes: aggregationViews.split(','), fieldProjections };
			const pipeline = clientAggregationPipelineBuilder(pipelineConfig);
			const a = await mongo.db
				.collection(config?.matAggCollectionName || 'materialisedAggregations')
				.aggregate(pipeline)
				.toArray();
			//////////////////////////////////////////////////////////////////////////
			// Validate aggregation result
			if (!a || a.length === 0 || !a[0]?.results) {
				send500(res, {
					message: `Error fetching data: no aggregation result returned`,
					errorCode: 'WDxxx', // TODO: Error codes should be documented in a central location and not as magic numbers in code
					category: 'Database Query Error',
				});
				return;
			}
			const retDoc = {
				requestedAggregationViews: aggregationViews.split(','),
				requestedAggregationEdges: aggregationEdges.split(','),
				requestedAggregationMax: aggregationMax,
			};
			let totalCount = 0;
			const API_URL = config.express.fullHostUrl + `${req.params.apiVersion || '1-0'}/${schemaType}`;
			const results = a[0].results;
			const aggregations = {};
			//////////////////////////////////////////////////////////////////////////
			// Create the output aggregations document
			for (const key in results) {
				aggregations[key] = {};
				aggregations[key].items = [];
				aggregations[key].nextPages = [];
				////////////////////////////////////////////////////////////////////////
				// Process materialised into the aggregations object
				if (Array.isArray(results[key]?.items)) {
					totalCount += results[key].items.length;
					for (const item of results[key].items) {
						if (item._original && req.query.includeOriginal !== true && req.query.includeOriginal !== 'true') delete item._original;
						if (item._stickies && req.query.includeStickies !== true && req.query.includeStickies !== 'true') delete item._stickies;
						aggregations[key].items.push(item);
					}
				}
				////////////////////////////////////////////////////////////////////////
				// Process overflow IDs into next page query strings
				let extraQueryStrings = ``;
				if (req.query.includeOriginal === true || req.query.includeOriginal === 'true') extraQueryStrings += `&includeOriginal=true`;
				if (req.query.includeStickies === true || req.query.includeStickies === 'true') extraQueryStrings += `&includeStickies=true`;
				if (Array.isArray(results[key]?.overflow?.overflowIds)) {
					totalCount += results[key].overflow.overflowIds.length;
					results[key].overflow.nextPages = [];
					for (const chunk of chunkAndFormatObjectIDs(results[key].overflow.overflowIds, aggregationMax)) {
						if (chunk.length > 0) aggregations[key].nextPages.push(`${API_URL}?query=(or ${chunk})${extraQueryStrings}`);
					}
				}
			}

			retDoc.totalCount = totalCount;
			retDoc.rootDocument = r;
			retDoc.aggregations = aggregations;
			retDoc.builtAggregationConfig = pipelineConfig;
			info(`Aggregation completed with ${totalCount} aggregated resources`, id);
			send200(res, retDoc);
			return;
		}
		////////////////////////////////////////////////////////////////////////////
		// We are not attempting an aggregation
		else {
			info(`No aggregation requested, returning raw data`, id);
			send200(res, r);
			return;
		}
	} catch (e) {
		//////////////////////////////////////////////////////////////////////////////
		// Catch any DB query errors
		warn(`Error fetching single sports data for schemaType: ${schemaType}, scope: ${scope}, id: ${requestedId} - ${e.message}`, 'WD0060', 500, 'Database Query Error');
		send500(res, {
			message: `Error fetching data: ${e.message}`,
			errorCode: 'WDxxx', // TODO: Error codes should be documented in a central location and not as magic numbers in code
			category: 'Database Query Error',
		});
		return;
	}
}
////////////////////////////////////////////////////////////////////////////////
/**
 * Splits an array of MongoDB ObjectIDs into chunks and formats them as query strings
 * @param {Array} objectIdArray - Array of MongoDB ObjectIDs
 * @param {number} maxSize - Maximum size for each chunk
 * @returns {Array<string>} Array of formatted query strings
 */
function chunkAndFormatObjectIDs(objectIdArray, maxSize) {
	if (!Array.isArray(objectIdArray) || objectIdArray.length === 0) return [];
	if (!maxSize || maxSize <= 0) throw new Error('aggregationMax must be a positive number');
	const chunks = [];
	//////////////////////////////////////////////////////////////////////////////
	// Split array into chunks
	for (let i = 0; i < objectIdArray.length; i += maxSize) {
		const chunk = objectIdArray.slice(i, i + maxSize);
		////////////////////////////////////////////////////////////////////////////
		// Convert each ObjectID to the formatted string
		const formattedChunk = chunk.map((objectId) => `_id==\`${objectId.toString()}\``).join(' ');
		chunks.push(formattedChunk);
	}
	//////////////////////////////////////////////////////////////////////////////
	return chunks;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { getSingleSportsData };
