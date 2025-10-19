const _ = require('lodash');
const { rankingMetaFacet } = require('./rankingMetaFacet.js');
const { rankingTeamsFacet } = require('./rankingTeamsFacet.js');
const { rankingSportsPersonsFacet } = require('./rankingSportsPersonsFacet.js');
const { rankingStagesFacet } = require('./rankingStagesFacet.js');
const { rankingEventsFacet } = require('./rankingEventsFacet.js');
const {
	keySeparator,
	keyInAggregation,
	rankingLabelSeparator,
	rankingPositionSeparator,
	rankingStageTeamSeparator,
	rankingStageSportsPersonSeparator,
	rankingEventTeamSeparator,
	rankingEventSportsPersonSeparator,
} = require('../constants.js');

////////////////////////////////////////////////////////////////////////////////
class RankingKeyClass {
	//////////////////////////////////////////////////////////////////////////////
	constructor({
		sportsPersonId,
		sportsPersonIdScope,
		teamId,
		teamIdScope,
		clubId,
		clubIdScope,
		nationId,
		nationIdScope,
		stageId,
		stageIdScope,
		eventId,
		eventIdScope,
		dateTimeLabel,
		ranking,
	}) {
		this.sportsPersonId = sportsPersonId;
		this.sportsPersonIdScope = sportsPersonIdScope;
		this.teamId = teamId;
		this.teamIdScope = teamIdScope;
		this.clubId = clubId;
		this.clubIdScope = clubIdScope;
		this.nationId = nationId;
		this.nationIdScope = nationIdScope;
		this.stageId = stageId;
		this.stageIdScope = stageIdScope;
		this.eventId = eventId;
		this.eventIdScope = eventIdScope;
		this.dateTimeLabel = dateTimeLabel;
		this.ranking = ranking;
	}

	//////////////////////////////////////////////////////////////////////////////
	validate() {
		const hasValidTeam = _.isString(this.teamId) && _.isString(this.teamIdScope);
		const hasValidSportsPerson = _.isString(this.sportsPersonId) && _.isString(this.sportsPersonIdScope);
		const hasValidStage = _.isString(this.stageId) && _.isString(this.stageIdScope);
		const hasValidEvent = _.isString(this.eventId) && _.isString(this.eventIdScope);
		const hasValidDateTimeLabel = _.isString(this.dateTimeLabel);
		const hasValidRanking = _.isNumber(this.ranking);
		if (!hasValidSportsPerson && !hasValidTeam) return false;
		if (!hasValidStage && !hasValidEvent) return false;
		if (!hasValidDateTimeLabel) return false;
		if (!hasValidRanking) return false;
		return true;
	}

	//////////////////////////////////////////////////////////////////////////////
	type() {
		if (_.isString(this.teamId) && _.isString(this.teamIdScope) && _.isString(this.stageId) && _.isString(this.stageIdScope)) return 'stageTeamRanking';
		if (_.isString(this.sportsPersonId) && _.isString(this.sportsPersonIdScope) && _.isString(this.stageId) && _.isString(this.stageIdScope))
			return 'stageSportsPersonRanking';
		if (_.isString(this.teamId) && _.isString(this.teamIdScope) && _.isString(this.eventId) && _.isString(this.eventIdScope)) return 'eventTeamRanking';
		if (_.isString(this.sportsPersonId) && _.isString(this.sportsPersonIdScope) && _.isString(this.eventId) && _.isString(this.eventIdScope))
			return 'eventSportsPersonRanking';
		return null;
	}

	//////////////////////////////////////////////////////////////////////////////
	rankingDocumentQuery() {
		const rankingType = this.type();
		if (rankingType === 'stageTeamRanking') return this.rankingStageTeamHelpers().query;
		if (rankingType === 'stageSportsPersonRanking') return this.rankingStageSportsPersonHelpers().query;
		if (rankingType === 'eventTeamRanking') return this.rankingEventTeamHelpers().query;
		if (rankingType === 'eventSportsPersonRanking') return this.rankingEventSportsPersonHelpers().query;
		return null;
	}

	//////////////////////////////////////////////////////////////////////////////
	aggregationDocQuery = () => {
		const rankingType = this.type();
		if (rankingType === 'stageTeamRanking') return { resourceType: 'ranking', externalKey: this.rankingStageTeamHelpers().key };
		if (rankingType === 'stageSportsPersonRanking') return { resourceType: 'ranking', externalKey: this.rankingStageSportsPersonHelpers().key };
		if (rankingType === 'eventTeamRanking') return { resourceType: 'ranking', externalKey: this.rankingEventTeamHelpers().key };
		if (rankingType === 'eventSportsPersonRanking') return { resourceType: 'ranking', externalKey: this.rankingEventSportsPersonHelpers().key };
		return null;
	};

	//////////////////////////////////////////////////////////////////////////////
	match() {
		const rankingType = this.type();
		if (rankingType === 'stageTeamRanking') {
			return rankingStageTeam(this.stageId, this.stageIdScope, this.teamId, this.teamIdScope, this.dateTimeLabel, this.ranking).match;
		}
		if (rankingType === 'stageSportsPersonRanking') {
			return rankingStageSportsPerson(this.stageId, this.stageIdScope, this.sportsPersonId, this.sportsPersonIdScope, this.dateTimeLabel, this.ranking).match;
		}
		if (rankingType === 'eventTeamRanking') {
			return rankingEventTeam(this.eventId, this.eventIdScope, this.teamId, this.teamIdScope, this.dateTimeLabel, this.ranking).match;
		}
		if (rankingType === 'eventSportsPersonRanking') {
			return rankingEventSportsPerson(this.eventId, this.eventIdScope, this.sportsPersonId, this.sportsPersonIdScope, this.dateTimeLabel, this.ranking).match;
		}
		return null;
	}

	//////////////////////////////////////////////////////////////////////////////
	report() {
		const rankingType = this.type();
		if (rankingType === 'stageTeamRanking') {
			return `stageTeamRanking for stageId: ${this.stageId}, stageIdScope: ${this.stageIdScope}, teamId: ${this.teamId}, teamIdScope: ${this.teamIdScope}, dateTimeLabel: ${this.dateTimeLabel}, ranking: ${this.ranking}`;
		}
		if (rankingType === 'stageSportsPersonRanking') {
			return `stageSportsPersonRanking for stageId: ${this.stageId}, stageIdScope: ${this.stageIdScope}, sportsPersonId: ${this.sportsPersonId}, sportsPersonIdScope: ${this.sportsPersonIdScope}, dateTimeLabel: ${this.dateTimeLabel}, ranking: ${this.ranking}`;
		}
		if (rankingType === 'eventTeamRanking') {
			return `eventTeamRanking for eventId: ${this.eventId}, eventIdScope: ${this.eventIdScope}, teamId: ${this.teamId}, teamIdScope: ${this.teamIdScope}, dateTimeLabel: ${this.dateTimeLabel}, ranking: ${this.ranking}`;
		}
		if (rankingType === 'eventSportsPersonRanking') {
			return `eventSportsPersonRanking for eventId: ${this.eventId}, eventIdScope: ${this.eventIdScope}, sportsPersonId: ${this.sportsPersonId}, sportsPersonIdScope: ${this.sportsPersonIdScope}, dateTimeLabel: ${this.dateTimeLabel}, ranking: ${this.ranking}`;
		}
		return null;
	}

	////////////////////////////////////////////////////////////////////////////////
	pipelineStageTeam = (config) => [
		{ $match: this.rankingStageTeamHelpers(this.stageId, this.stageIdScope, this.teamId, this.teamIdScope, this.dateTimeLabel, this.ranking).match },
		{ $facet: { meta: rankingMetaFacet, teams: rankingTeamsFacet, stages: rankingStagesFacet } },
		{
			$project: {
				meta: { $ifNull: [{ $first: '$meta' }, null] },
				teams: { $ifNull: [{ $first: '$teams' }, null] },
				stages: {
					$ifNull: [{ $first: '$stages' }, null],
				},
			},
		},
		{
			$project: {
				resourceType: '$meta.resourceType',
				externalKey: {
					$cond: [
						this.rankingStageTeamHelpers(this.stageId, this.stageIdScope, this.teamId, this.teamIdScope, this.dateTimeLabel, this.ranking).matchWithMeta,
						this.rankingStageTeamHelpers(this.stageId, this.stageIdScope, this.teamId, this.teamIdScope, this.dateTimeLabel, this.ranking).keyWithMeta,
						null,
					],
				},
				gamedayId: '$meta._id',
				_externalTeamId: '$meta._externalTeamId',
				_externalTeamIdScope: '$meta._externalTeamIdScope',
				_externalSportsPersonId: null,
				_externalSportsPersonIdScope: null,
				_externalStageId: '$meta._externalStageId',
				_externalStageIdScope: '$meta._externalStageIdScope',
				_externalEventId: null,
				_externalEventIdScope: null,
				dateTime: '$meta.dateTime',
				ranking: '$meta.ranking',
				name: '$meta.name',
				stages: { $ifNull: ['$stages.ids', []] },
				stageKeys: { $ifNull: ['$stages.keys', []] },
				events: [],
				eventKeys: [],
				teams: { $ifNull: ['$teams.ids', []] },
				teamKeys: { $ifNull: ['$teams.keys', []] },
				sportsPersons: [],
				sportsPersonKeys: [],
				lastUpdated: '$$NOW',
			},
		},
		{
			$match: {
				externalKey: { $ne: null },
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
	pipelineStageSportsPerson = (config) => [
		{ $match: this.rankingStageSportsPersonHelpers(this.stageId, this.stageIdScope, this.sportsPersonId, this.sportsPersonIdScope, this.dateTimeLabel, this.ranking).match },
		{ $facet: { meta: rankingMetaFacet, sportsPersons: rankingSportsPersonsFacet, stages: rankingStagesFacet } },
		{
			$project: {
				meta: { $ifNull: [{ $first: '$meta' }, null] },
				teams: { $ifNull: [{ $first: '$teams' }, null] },
				stages: {
					$ifNull: [{ $first: '$stages' }, null],
				},
			},
		},
		{
			$project: {
				resourceType: '$meta.resourceType',
			},
			externalKey: {
				$cond: [
					this.rankingStageSportsPersonHelpers(this.stageId, this.stageIdScope, this.sportsPersonId, this.sportsPersonIdScope, this.dateTimeLabel, this.ranking).matchWithMeta,
					this.rankingStageSportsPersonHelpers(this.stageId, this.stageIdScope, this.sportsPersonId, this.sportsPersonIdScope, this.dateTimeLabel, this.ranking).keyWithMeta,
					null,
				],
			},
			gamedayId: '$meta._id',
			_externalTeamId: null,
			_externalTeamIdScope: null,
			_externalSportsPersonId: '$meta._externalSportsPersonId',
			_externalSportsPersonIdScope: '$meta._externalSportsPersonIdScope',
			_externalStageId: '$meta._externalStageId',
			_externalStageIdScope: '$meta._externalStageIdScope',
			_externalEventId: null,
			_externalEventIdScope: null,
			dateTime: '$meta.dateTime',
			ranking: '$meta.ranking',
			name: '$meta.name',
			stages: { $ifNull: ['$stages.ids', []] },
			stageKeys: { $ifNull: ['$stages.keys', []] },
			events: [],
			eventKeys: [],
			teams: [],
			teamKeys: [],
			sportsPersons: { $ifNull: ['$sportsPersons.ids', []] },
			sportsPersonKeys: { $ifNull: ['$sportsPersons.keys', []] },
			lastUpdated: '$$NOW',
		},
		{
			$match: {
				externalKey: { $ne: null },
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
	pipelineEventTeam = (config) => [
		{ $match: this.rankingEventTeamHelpers(this.eventId, this.eventIdScope, this.teamId, this.teamIdScope, this.dateTimeLabel, this.ranking).match },
		{ $facet: { meta: rankingMetaFacet, teams: rankingTeamsFacet, events: rankingEventsFacet } },
		{
			$project: {
				meta: { $ifNull: [{ $first: '$meta' }, null] },
				teams: { $ifNull: [{ $first: '$teams' }, null] },
				stages: {
					$ifNull: [{ $first: '$stages' }, null],
				},
			},
		},
		{
			$project: {
				resourceType: '$meta.resourceType',
			},
			externalKey: {
				$cond: [
					this.rankingEventTeamHelpers(this.eventId, this.eventIdScope, this.teamId, this.teamIdScope, this.dateTimeLabel, this.ranking).matchWithMeta,
					this.rankingEventTeamHelpers(this.eventId, this.eventIdScope, this.teamId, this.teamIdScope, this.dateTimeLabel, this.ranking).keyWithMeta,
					null,
				],
			},
			gamedayId: '$meta._id',
			_externalTeamId: '$meta._externalTeamId',
			_externalTeamIdScope: '$meta._externalTeamIdScope',
			_externalSportsPersonId: null,
			_externalSportsPersonIdScope: null,
			_externalStageId: null,
			_externalStageIdScope: null,
			_externalEventId: '$meta._externalEventId',
			_externalEventIdScope: '$meta._externalEventIdScope',
			dateTime: '$meta.dateTime',
			ranking: '$meta.ranking',
			name: '$meta.name',
			stages: [],
			stageKeys: [],
			events: { $ifNull: ['$events.ids', []] },
			eventKeys: { $ifNull: ['$events.keys', []] },
			teams: { $ifNull: ['$teams.ids', []] },
			teamKeys: { $ifNull: ['$teams.keys', []] },
			sportsPersons: [],
			sportsPersonKeys: [],
			lastUpdated: '$$NOW',
		},

		{
			$match: {
				externalKey: { $ne: null },
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
	pipelineEventSportsPerson = (config) => [
		{ $match: this.rankingEventSportsPersonHelpers(this.eventId, this.eventIdScope, this.sportsPersonId, this.sportsPersonIdScope, this.dateTimeLabel, this.ranking).match },
		{ $facet: { meta: rankingMetaFacet, sportsPersons: rankingSportsPersonsFacet, events: rankingEventsFacet } },
		{
			$project: {
				meta: { $ifNull: [{ $first: '$meta' }, null] },
				teams: { $ifNull: [{ $first: '$teams' }, null] },
				stages: {
					$ifNull: [{ $first: '$stages' }, null],
				},
			},
		},
		{
			$project: {
				resourceType: '$meta.resourceType',
			},
			externalKey: {
				$cond: [
					this.rankingEventSportsPersonHelpers(this.eventId, this.eventIdScope, this.sportsPersonId, this.sportsPersonIdScope, this.dateTimeLabel, this.ranking).matchWithMeta,
					this.rankingEventSportsPersonHelpers(this.eventId, this.eventIdScope, this.sportsPersonId, this.sportsPersonIdScope, this.dateTimeLabel, this.ranking).keyWithMeta,
					null,
				],
			},
			gamedayId: '$meta._id',
			_externalTeamId: null,
			_externalTeamIdScope: null,
			_externalSportsPersonId: '$meta._externalSportsPersonId',
			_externalSportsPersonIdScope: '$meta._externalSportsPersonIdScope',
			_externalStageId: null,
			_externalStageIdScope: null,
			_externalEventId: '$meta._externalEventId',
			_externalEventIdScope: '$meta._externalEventIdScope',
			dateTime: '$meta.dateTime',
			ranking: '$meta.ranking',
			name: '$meta.name',
			stages: [],
			stageKeys: [],
			events: { $ifNull: ['$events.ids', []] },
			eventKeys: { $ifNull: ['$events.keys', []] },
			teams: [],
			teamKeys: [],
			sportsPersons: { $ifNull: ['$sportsPersons.ids', []] },
			sportsPersonKeys: { $ifNull: ['$sportsPersons.keys', []] },
			lastUpdated: '$$NOW',
		},
		{
			$match: {
				externalKey: { $ne: null },
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

	//////////////////////////////////////////////////////////////////////////////
	pipeline = (config) => {
		const type = this.type();
		switch (type) {
			case 'stageTeamRanking':
				return this.pipelineStageTeam(config, this.stageId, this.stageIdScope, this.teamId, this.teamIdScope, this.dateTimeLabel, this.ranking);
			case 'eventTeamRanking':
				return this.pipelineEventTeam(config, this.eventId, this.eventIdScope, this.teamId, this.teamIdScope, this.dateTimeLabel, this.ranking);
			case 'stageSportsPersonRanking':
				return this.pipelineStageSportsPerson(config, this.stageId, this.stageIdScope, this.sportsPersonId, this.sportsPersonIdScope, this.dateTimeLabel, this.ranking);
			case 'eventSportsPersonRanking':
				return this.pipelineEventSportsPerson(config, this.eventId, this.eventIdScope, this.sportsPersonId, this.sportsPersonIdScope, this.dateTimeLabel, this.ranking);
			default:
				return [];
		}
	};

	////////////////////////////////////////////////////////////////////////////////
	rankingStageTeamHelpers = () => {
		return {
			key: `${this.stageId || ''}${keySeparator}${this.stageIdScope || ''}${rankingStageTeamSeparator}${this.teamId || ''}${keySeparator}${
				this.teamIdScope || ''
			}${rankingLabelSeparator}${this.dateTimeLabel || ''}${rankingPositionSeparator}${this.ranking || ''}`,
			match: {
				$and: [
					{ _externalStageId: this.stageId },
					{ _externalStageIdScope: this.stageIdScope },
					{ _externalTeamId: this.teamId },
					{ _externalTeamIdScope: this.teamIdScope },
					{ dateTime: this.dateTimeLabel.toString() },
					{ ranking: Number(this.ranking) },
				],
			},
			keyWithMeta: {
				$concat: [
					'$meta._externalStageId',
					keySeparator,
					'$meta._externalStageIdScope',
					rankingStageTeamSeparator,
					'$meta._externalTeamId',
					keySeparator,
					'$meta._externalTeamIdScope',
					rankingLabelSeparator,
					'$meta.dateTime',
					rankingPositionSeparator,
					{ $toString: '$meta.ranking' },
				],
			},
			matchWithMeta: {
				$and: [
					{ $eq: ['$meta._externalStageId', this.stageId] },
					{ $eq: ['$meta._externalStageIdScope', this.stageIdScope] },
					{ $eq: ['$meta._externalTeamId', this.teamId] },
					{ $eq: ['$meta._externalTeamIdScope', this.teamIdScope] },
					{ $eq: ['$meta.dateTime', this.dateTimeLabel.toString()] },
					{ $eq: ['$meta.ranking', Number(this.ranking)] },
				],
			},
			query: {
				_externalStageId: this.stageId,
				_externalStageIdScope: this.stageIdScope,
				_externalTeamId: this.teamId,
				_externalTeamIdScope: this.teamIdScope,
				dateTime: this.dateTimeLabel.toString(),
				ranking: Number(this.ranking),
			},
		};
	};

	////////////////////////////////////////////////////////////////////////////////
	rankingStageSportsPersonHelpers = () => {
		return {
			key: `${this.stageId || ''}${keySeparator}${this.stageIdScope || ''}${rankingStageSportsPersonSeparator}${this.sportsPersonId || ''}${keySeparator}${
				this.sportsPersonIdScope || ''
			}${rankingLabelSeparator}${this.dateTimeLabel || ''}${rankingPositionSeparator}${this.ranking || ''}`,
			match: {
				$and: [
					{ _externalStageId: this.stageId },
					{ _externalStageIdScope: this.stageIdScope },
					{ _externalSportsPersonId: this.sportsPersonId },
					{ _externalSportsPersonIdScope: this.sportsPersonIdScope },
					{ dateTime: this.dateTimeLabel.toString() },
					{ ranking: Number(this.ranking) },
				],
			},
			keyWithMeta: {
				$concat: [
					'$meta._externalStageId',
					keySeparator,
					'$meta._externalStageIdScope',
					rankingStageSportsPersonSeparator,
					'$meta._externalSportsPersonId',
					keySeparator,
					'$meta._externalSportsPersonIdScope',
					rankingLabelSeparator,
					'$meta.dateTime',
					rankingPositionSeparator,
					{ $toString: '$meta.ranking' },
				],
			},
			matchWithMeta: {
				$and: [
					{ $eq: ['$meta._externalStageId', this.stageId] },
					{ $eq: ['$meta._externalStageIdScope', this.stageIdScope] },
					{ $eq: ['$meta._externalSportsPersonId', this.sportsPersonId] },
					{ $eq: ['$meta._externalSportsPersonIdScope', this.sportsPersonIdScope] },
					{ $eq: ['$meta.dateTime', this.dateTimeLabel.toString()] },
					{ $eq: ['$meta.ranking', Number(this.ranking)] },
				],
			},
			query: {
				_externalStageId: this.stageId,
				_externalStageIdScope: this.stageIdScope,
				_externalSportsPersonId: this.sportsPersonId,
				_externalSportsPersonIdScope: this.sportsPersonIdScope,
				dateTime: this.dateTimeLabel.toString(),
				ranking: Number(this.ranking),
			},
		};
	};

	////////////////////////////////////////////////////////////////////////////////
	rankingEventTeamHelpers = () => {
		return {
			key: `${this.eventId || ''}${keySeparator}${this.eventIdScope || ''}${rankingEventTeamSeparator}${this.teamId || ''}${keySeparator}${
				this.teamIdScope || ''
			}${rankingLabelSeparator}${this.dateTimeLabel || ''}${rankingPositionSeparator}${this.ranking || ''}`,
			match: {
				$and: [
					{ _externalEventId: this.eventId },
					{ _externalEventIdScope: this.eventIdScope },
					{ _externalTeamId: this.teamId },
					{ _externalTeamIdScope: this.teamIdScope },
					{ dateTime: this.dateTimeLabel.toString() },
					{ ranking: Number(this.ranking) },
				],
			},
			keyWithMeta: {
				$concat: [
					'$meta._externalEventId',
					keySeparator,
					'$meta._externalEventIdScope',
					rankingEventTeamSeparator,
					'$meta._externalTeamId',
					keySeparator,
					'$meta._externalTeamIdScope',
					rankingLabelSeparator,
					'$meta.dateTime',
					rankingPositionSeparator,
					{ $toString: '$meta.ranking' },
				],
			},
			matchWithMeta: {
				$and: [
					{ $eq: ['$meta._externalEventId', this.eventId] },
					{ $eq: ['$meta._externalEventIdScope', this.eventIdScope] },
					{ $eq: ['$meta._externalTeamId', this.teamId] },
					{ $eq: ['$meta._externalTeamIdScope', this.teamIdScope] },
					{ $eq: ['$meta.dateTime', this.dateTimeLabel.toString()] },
					{ $eq: ['$meta.ranking', Number(this.ranking)] },
				],
			},
			query: {
				_externalEventId: this.eventId,
				_externalEventIdScope: this.eventIdScope,
				_externalTeamId: this.teamId,
				_externalTeamIdScope: this.teamIdScope,
				dateTime: this.dateTimeLabel.toString(),
				ranking: Number(this.ranking),
			},
		};
	};

	////////////////////////////////////////////////////////////////////////////////
	rankingEventSportsPersonHelpers = () => {
		return {
			key: `${this.eventId || ''}${keySeparator}${this.eventIdScope || ''}${rankingEventSportsPersonSeparator}${this.sportsPersonId || ''}${keySeparator}${
				this.sportsPersonIdScope || ''
			}${rankingLabelSeparator}${this.dateTimeLabel || ''}${rankingPositionSeparator}${this.ranking || ''}`,
			match: {
				$and: [
					{ _externalEventId: this.eventId },
					{ _externalEventIdScope: this.eventIdScope },
					{ _externalSportsPersonId: this.sportsPersonId },
					{ _externalSportsPersonIdScope: this.sportsPersonIdScope },
					{ dateTime: this.dateTimeLabel.toString() },
					{ ranking: Number(this.ranking) },
				],
			},
			keyWithMeta: {
				$concat: [
					'$meta._externalEventId',
					keySeparator,
					'$meta._externalEventIdScope',
					rankingEventSportsPersonSeparator,
					'$meta._externalSportsPersonId',
					keySeparator,
					'$meta._externalSportsPersonIdScope',
					rankingLabelSeparator,
					'$meta.dateTime',
					rankingPositionSeparator,
					{ $toString: '$meta.ranking' },
				],
			},
			matchWithMeta: {
				$and: [
					{ $eq: ['$meta._externalEventId', this.eventId] },
					{ $eq: ['$meta._externalEventIdScope', this.eventIdScope] },
					{ $eq: ['$meta._externalSportsPersonId', this.sportsPersonId] },
					{ $eq: ['$meta._externalSportsPersonIdScope', this.sportsPersonIdScope] },
					{ $eq: ['$meta.dateTime', this.dateTimeLabel.toString()] },
					{ $eq: ['$meta.ranking', Number(this.ranking)] },
				],
			},
			query: {
				_externalEventId: this.eventId,
				_externalEventIdScope: this.eventIdScope,
				_externalSportsPersonId: this.sportsPersonId,
				_externalSportsPersonIdScope: this.sportsPersonIdScope,
				dateTime: this.dateTimeLabel.toString(),
				ranking: Number(this.ranking),
			},
		};
	};
}

module.exports = { RankingKeyClass };
