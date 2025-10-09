const { keySeparator } = require('../constants');
////////////////////////////////////////////////////////////////////////////////
const competitionVenuesFacet = [
	// 1) Stages for this competition -> stageKeys
	{
		$lookup: {
			from: 'stages',
			let: { cid: '$_externalId', cs: '$_externalIdScope' },
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [{ $eq: ['$_externalCompetitionId', '$$cid'] }, { $eq: ['$_externalCompetitionIdScope', '$$cs'] }],
						},
					},
				},
				{
					$project: {
						_id: 0,
						stageKey: { $concat: ['$_externalId', ' @ ', '$_externalIdScope'] },
					},
				},
			],
			as: 'stages',
		},
	},
	{
		$project: {
			stageKeys: {
				$setUnion: [{ $map: { input: '$stages', as: 's', in: '$$s.stageKey' } }, []],
			},
		},
	},

	// 2) Events in those stages -> extract venue pairs (non-empty strings), de-dupe without $unwind
	{
		$lookup: {
			from: 'events',
			let: { stageKeys: '$stageKeys' },
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [
								{ $gt: [{ $size: '$$stageKeys' }, 0] },
								{
									$in: [{ $concat: ['$_externalStageId', ' @ ', '$_externalStageIdScope'] }, '$$stageKeys'],
								},
							],
						},
					},
				},
				{
					// Build a tiny array per event: [] or [ { id, scope, key } ]
					$project: {
						pairs: {
							$cond: [
								{
									$and: [
										{ $eq: [{ $type: '$_externalVenueId' }, 'string'] },
										{ $ne: ['$_externalVenueId', ''] },
										{ $eq: [{ $type: '$_externalVenueIdScope' }, 'string'] },
										{ $ne: ['$_externalVenueIdScope', ''] },
									],
								},
								[
									{
										id: '$_externalVenueId',
										scope: '$_externalVenueIdScope',
										key: { $concat: ['$_externalVenueId', ' @ ', '$_externalVenueIdScope'] },
									},
								],
								[],
							],
						},
					},
				},
				// Merge & dedupe across ALL matched events
				{ $group: { _id: null, arrs: { $push: '$pairs' } } },
				{
					$project: {
						_id: 0,
						pairs: {
							$reduce: {
								input: '$arrs',
								initialValue: [],
								in: { $setUnion: ['$$value', '$$this'] },
							},
						},
					},
				},
				{ $limit: 1 },
			],
			as: 'venueAgg',
		},
	},

	// 3) Pull unique venue pairs ([] if none)
	{
		$project: {
			venuePairs: {
				$ifNull: [{ $getField: { field: 'pairs', input: { $first: '$venueAgg' } } }, []],
			},
		},
	},

	// 4) Lookup venues by composite key (no unwind)
	{
		$lookup: {
			from: 'venues',
			let: {
				vKeys: {
					$setUnion: [{ $map: { input: '$venuePairs', as: 'v', in: '$$v.key' } }, []],
				},
			},
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [
								{ $gt: [{ $size: '$$vKeys' }, 0] },
								{
									$in: [{ $concat: ['$_externalId', ' @ ', '$_externalIdScope'] }, '$$vKeys'],
								},
							],
						},
					},
				},
				{
					$project: {
						_id: 1,
						key: { $concat: ['$_externalId', ' @ ', '$_externalIdScope'] },
					},
				},
			],
			as: 'venuesHit',
		},
	},

	// 5) Final shape: ids + venueKeys (deduped, null-safe)
	{
		$project: {
			_id: 0,
			ids: {
				$setUnion: [{ $map: { input: '$venuesHit', as: 'v', in: '$$v._id' } }, []],
			},
			venueKeys: {
				$setUnion: [{ $map: { input: '$venuePairs', as: 'v', in: '$$v.key' } }, []],
			},
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.competitionVenuesFacet = competitionVenuesFacet;
