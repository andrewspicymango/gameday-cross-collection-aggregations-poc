const pipeline = (COMP_SCOPE, COMP_ID) => [
	{ $match: { _externalId: COMP_ID, _externalIdScope: COMP_SCOPE } },

	//////////////////////////////////////////////////////////////////////////////
	// -------- Derive everything in one pass with precise, selective lookups
	{
		$facet: {
			//////////////////////////////////////////////////////////////////////////
			// SGOs directly on the competition
			sgos: [
				{ $project: { sgoMemberships: 1 } },
				{ $unwind: { path: '$sgoMemberships', preserveNullAndEmptyArrays: true } },
				{
					$lookup: {
						from: 'sgos',
						let: {
							sid: '$sgoMemberships._externalSgoId',
							sscope: '$sgoMemberships._externalSgoIdScope',
						},
						pipeline: [{ $match: { $expr: { $and: [{ $eq: ['$_externalId', '$$sid'] }, { $eq: ['$_externalIdScope', '$$sscope'] }] } } }, { $project: { _id: 1 } }],
						as: 'sgo',
					},
				},
				{ $unwind: { path: '$sgo', preserveNullAndEmptyArrays: true } },
				{ $group: { _id: null, ids: { $addToSet: '$sgo._id' } } },
				{ $project: { _id: 0, ids: 1 } },
			],

			// Stages in this competition
			stages: [
				{
					$lookup: {
						from: 'stages',
						let: { cid: '$_externalId', cs: '$_externalIdScope' },
						pipeline: [
							{ $match: { $expr: { $and: [{ $eq: ['$_externalCompetitionId', '$$cid'] }, { $eq: ['$_externalCompetitionIdScope', '$$cs'] }] } } },
							{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
						],
						as: 'stages',
					},
				},
				{ $unwind: { path: '$stages', preserveNullAndEmptyArrays: true } },
				{ $group: { _id: null, ids: { $addToSet: '$stages._id' }, stageKeys: { $addToSet: { id: '$stages._externalId', scope: '$stages._externalIdScope' } } } },
				{ $project: { _id: 0, ids: 1, stageKeys: 1 } },
			],

			// Events for those stages
			events: [
				{
					$lookup: {
						from: 'stages',
						let: { cid: '$_externalId', cs: '$_externalIdScope' },
						pipeline: [
							{ $match: { $expr: { $and: [{ $eq: ['$_externalCompetitionId', '$$cid'] }, { $eq: ['$_externalCompetitionIdScope', '$$cs'] }] } } },
							{ $project: { _externalId: 1, _externalIdScope: 1 } },
						],
						as: 'stages',
					},
				},
				{
					$addFields: {
						_stageKeys: {
							$map: {
								input: '$stages',
								as: 's',
								in: { $concat: ['$$s._externalId', '|', '$$s._externalIdScope'] },
							},
						},
					},
				},
				{
					$lookup: {
						from: 'events',
						let: { keys: '$_stageKeys' },
						pipeline: [
							{
								$match: {
									$expr: {
										$in: [{ $concat: ['$_externalStageId', '|', '$_externalStageIdScope'] }, '$$keys'],
									},
								},
							},
							{
								$project: {
									_id: 1,
									_externalVenueId: 1,
									_externalVenueIdScope: 1,
									participants: 1,
								},
							},
						],
						as: 'events',
					},
				},
				{ $project: { events: '$events' } },
			],
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	// -------- Extract arrays for events, venues, teams, sportsPeople
	{
		$project: {
			sgos: { $ifNull: [{ $first: '$sgos.ids' }, []] },

			// Stages
			stages: { $ifNull: [{ $first: '$stages.ids' }, []] },

			// Events
			events: {
				$ifNull: [
					{
						$setUnion: [
							{
								$map: {
									input: { $ifNull: [{ $first: '$events.events' }, []] },
									as: 'e',
									in: '$$e._id',
								},
							},
							[], // for shape
						],
					},
					[],
				],
			},

			// Venues (join by event’s external venue keys)
			_venueKeys: {
				$map: {
					input: { $ifNull: [{ $first: '$events.events' }, []] },
					as: 'e',
					in: {
						id: '$$e._externalVenueId',
						scope: '$$e._externalVenueIdScope',
					},
				},
			},

			// Participants for teams / sportsPeople
			_participants: {
				$map: {
					input: { $ifNull: [{ $first: '$events.events' }, []] },
					as: 'e',
					in: '$$e.participants',
				},
			},
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	// Flatten participants
	{ $unwind: { path: '$_participants', preserveNullAndEmptyArrays: true } },
	{ $unwind: { path: '$_participants', preserveNullAndEmptyArrays: true } },

	//////////////////////////////////////////////////////////////////////////////
	// Collect team/sportsPerson external keys
	{
		$group: {
			_id: null,
			sgos: { $first: '$sgos' },
			stages: { $first: '$stages' },
			events: { $first: '$events' },
			venueKeys: { $addToSet: '$_venueKeys' },
			teamKeys: {
				$addToSet: {
					id: '$_participants._externalTeamId',
					scope: '$_participants._externalTeamIdScope',
				},
			},
			spKeys: {
				$addToSet: {
					id: '$_participants._externalSportsPersonId',
					scope: '$_participants._externalSportsPersonIdScope',
				},
			},
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	// Normalize arrays and look up _id’s for venues, teams, sportsPeople
	{
		$project: {
			sgos: 1,
			stages: 1,
			events: 1,

			venueKeys: {
				$setDifference: [
					{
						$filter: {
							input: { $reduce: { input: '$venueKeys', initialValue: [], in: { $setUnion: ['$$value', '$$this'] } } },
							as: 'k',
							cond: { $and: [{ $ne: ['$$k.id', null] }, { $ne: ['$$k.scope', null] }] },
						},
					},
					[null],
				],
			},
			teamKeys: {
				$filter: {
					input: '$teamKeys',
					as: 'k',
					cond: { $and: [{ $ne: ['$$k.id', null] }, { $ne: ['$$k.scope', null] }] },
				},
			},
			spKeys: {
				$filter: {
					input: '$spKeys',
					as: 'k',
					cond: { $and: [{ $ne: ['$$k.id', null] }, { $ne: ['$$k.scope', null] }] },
				},
			},
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	// Look up _id values for venues/teams/sportsPeople
	{
		$lookup: {
			from: 'venues',
			let: { keys: '$venueKeys' },
			pipeline: [
				{
					$match: {
						$expr: { $in: [{ $concat: ['$_externalId', '|', '$_externalIdScope'] }, { $map: { input: '$$keys', as: 'k', in: { $concat: ['$$k.id', '|', '$$k.scope'] } } }] },
					},
				},
				{ $project: { _id: 1 } },
			],
			as: 'venueDocs',
		},
	},
	//////////////////////////////////////////////////////////////////////////////
	// Look up _id values for teams/sportsPeople
	{
		$lookup: {
			from: 'teams',
			let: { keys: '$teamKeys' },
			pipeline: [
				{
					$match: {
						$expr: { $in: [{ $concat: ['$_externalId', '|', '$_externalIdScope'] }, { $map: { input: '$$keys', as: 'k', in: { $concat: ['$$k.id', '|', '$$k.scope'] } } }] },
					},
				},
				{ $project: { _id: 1 } },
			],
			as: 'teamDocs',
		},
	},
	//////////////////////////////////////////////////////////////////////////////
	// Look up _id values for teams/sportsPeople
	{
		$lookup: {
			from: 'sportsPersons',
			let: { keys: '$spKeys' },
			pipeline: [
				{
					$match: {
						$expr: { $in: [{ $concat: ['$_externalId', '|', '$_externalIdScope'] }, { $map: { input: '$$keys', as: 'k', in: { $concat: ['$$k.id', '|', '$$k.scope'] } } }] },
					},
				},
				{ $project: { _id: 1 } },
			],
			as: 'spDocs',
		},
	},
	//////////////////////////////////////////////////////////////////////////////
	// Shape final doc
	{
		$project: {
			sgos: 1,
			stages: 1,
			events: 1,
			venues: { $ifNull: [{ $setUnion: ['$venueDocs._id', []] }, []] },
			teams: { $ifNull: [{ $setUnion: ['$teamDocs._id', []] }, []] },
			sportsPeople: { $ifNull: [{ $setUnion: ['$spDocs._id', []] }, []] },
		},
	},
	//////////////////////////////////////////////////////////////////////////////
	// Attach identity + timestamp
	{
		$addFields: {
			resourceType: 'competition',
			_externalIdScope: COMP_SCOPE,
			_externalId: COMP_ID,
			targetType: [`sgo`, `stage`, `event`, `venue`, `team`, `sportsPerson`].join('/'),
			lastUpdated: '$$NOW',
		},
	},

	// Merge into single doc keyed by (resourceType, scope, id)
	{ $merge: { into: 'materialisedAggregations', on: ['resourceType', '_externalIdScope', '_externalId', 'targetType'], whenMatched: 'replace', whenNotMatched: 'insert' } },
];

module.exports = pipeline;
