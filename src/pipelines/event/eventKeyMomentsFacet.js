const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const eventKeyMomentsFacet = [
	{
		$lookup: {
			from: 'keyMoments',
			let: {
				eid: '$_externalId',
				eids: '$_externalIdScope',
			},
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [
								{
									$eq: ['$_externalEventId', '$$eid'],
								},
								{
									$eq: ['$_externalEventIdScope', '$$eids'],
								},
							],
						},
					},
				},
				{
					$project: {
						_id: 1,
						keyMomentKey: {
							$concat: [
								{
									$ifNull: [
										{
											$toString: '$dateTime',
										},
										'',
									],
								},
								keySeparator,
								{
									$ifNull: ['$_externalEventIdScope', ''],
								},
								keySeparator,
								{
									$ifNull: ['$_externalEventId', ''],
								},
								keySeparator,
								{ $ifNull: ['$type', ''] },
								keySeparator,
								{
									$ifNull: ['$subType', ''],
								},
							],
						},
						teamParticipants: {
							$setUnion: [
								{
									$map: {
										input: {
											$filter: {
												input: {
													$ifNull: ['$participants', []],
												},
												as: 'p',
												cond: {
													$and: [
														{
															$ne: ['$$p._externalTeamId', null],
														},
														{
															$ne: ['$$p._externalTeamIdScope', null],
														},
														{
															$not: {
																$and: [
																	{
																		$eq: [
																			{
																				$type: '$$p._externalSportsPersonId',
																			},
																			'string',
																		],
																	},
																	{
																		$eq: [
																			{
																				$type: '$$p._externalSportsPersonIdScope',
																			},
																			'string',
																		],
																	},
																],
															},
														},
													],
												},
											},
										},
										as: 'p',
										in: {
											id: '$$p._externalTeamId',
											scope: '$$p._externalTeamIdScope',
											key: {
												$concat: ['$$p._externalTeamId', keySeparator, '$$p._externalTeamIdScope'],
											},
										},
									},
								},
								[],
							],
						},
						sportsPersonParticipants: {
							$setUnion: [
								{
									$map: {
										input: {
											$filter: {
												input: {
													$ifNull: ['$participants', []],
												},
												as: 'p',
												cond: {
													$and: [
														{
															$eq: [
																{
																	$type: '$$p._externalSportsPersonId',
																},
																'string',
															],
														},
														{
															$ne: ['$$p._externalSportsPersonId', ''],
														},
														{
															$eq: [
																{
																	$type: '$$p._externalSportsPersonIdScope',
																},
																'string',
															],
														},
														{
															$ne: ['$$p._externalSportsPersonIdScope', ''],
														},
													],
												},
											},
										},
										as: 'p',
										in: {
											id: '$$p._externalSportsPersonId',
											scope: '$$p._externalSportsPersonIdScope',
											key: {
												$concat: ['$$p._externalSportsPersonId', keySeparator, '$$p._externalSportsPersonIdScope'],
											},
										},
									},
								},
								[],
							],
						},
					},
				},
				{
					$project: {
						_id: 1,
						keyMomentKey: 1,
						teamParticipants: {
							$filter: {
								input: '$teamParticipants',
								as: 'sp',
								cond: {
									$ne: ['$$sp', null],
								},
							},
						},
						sportsPersonParticipants: {
							$filter: {
								input: '$sportsPersonParticipants',
								as: 'sp',
								cond: {
									$ne: ['$$sp', null],
								},
							},
						},
					},
				},
			],
			as: 'keyMoments',
		},
	},
	{
		$project: {
			keyMomentIds: {
				$map: {
					input: '$keyMoments',
					as: 'km',
					in: '$$km._id',
				},
			},
			keyMomentKeys: {
				$map: {
					input: '$keyMoments',
					as: 'km',
					in: '$$km.keyMomentKey',
				},
			},
			allTeamParticipants: {
				$reduce: {
					input: '$keyMoments.teamParticipants',
					initialValue: [],
					in: {
						$setUnion: ['$$value', '$$this'],
					},
				},
			},
			allSportsPersonParticipants: {
				$reduce: {
					input: '$keyMoments.sportsPersonParticipants',
					initialValue: [],
					in: {
						$setUnion: ['$$value', '$$this'],
					},
				},
			},
		},
	},
	{
		$lookup: {
			from: 'teams',
			let: {
				teamKeys: {
					$setUnion: [
						{
							$map: {
								input: '$allTeamParticipants',
								as: 't',
								in: '$$t.key',
							},
						},
						[],
					],
				},
			},
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [
								{
									$gt: [{ $size: '$$teamKeys' }, 0],
								},
								{
									$in: [
										{
											$concat: ['$_externalId', keySeparator, '$_externalIdScope'],
										},
										'$$teamKeys',
									],
								},
							],
						},
					},
				},
				{
					$project: {
						_id: 1,
						key: {
							$concat: ['$_externalId', keySeparator, '$_externalIdScope'],
						},
					},
				},
			],
			as: 'teamDocs',
		},
	},
	// Lookup sports person documents
	{
		$lookup: {
			from: 'sportsPersons',
			let: {
				spKeys: {
					$setUnion: [
						{
							$map: {
								input: '$allSportsPersonParticipants',
								as: 'sp',
								in: '$$sp.key',
							},
						},
						[],
					],
				},
			},
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [
								{
									$gt: [{ $size: '$$spKeys' }, 0],
								},
								{
									$in: [
										{
											$concat: ['$_externalId', keySeparator, '$_externalIdScope'],
										},
										'$$spKeys',
									],
								},
							],
						},
					},
				},
				{
					$project: {
						_id: 1,
						key: {
							$concat: ['$_externalId', keySeparator, '$_externalIdScope'],
						},
					},
				},
			],
			as: 'sportsPersonDocs',
		},
	},
	// Final projection
	{
		$project: {
			_id: 0,
			keyMomentIds: {
				$setUnion: ['$keyMomentIds', []],
			},
			keyMomentKeys: {
				$sortArray: {
					input: { $setUnion: ['$keyMomentKeys', []] },
					sortBy: 1, // 1 for ascending, -1 for descending
				},
			},
			teamIds: {
				$setUnion: [
					{
						$map: {
							input: '$teamDocs',
							as: 't',
							in: '$$t._id',
						},
					},
					[],
				],
			},
			teamKeys: {
				$setUnion: [
					{
						$map: {
							input: '$allTeamParticipants',
							as: 't',
							in: '$$t.key',
						},
					},
					[],
				],
			},
			sportsPersonIds: {
				$setUnion: [
					{
						$map: {
							input: '$sportsPersonDocs',
							as: 'sp',
							in: '$$sp._id',
						},
					},
					[],
				],
			},
			sportsPersonKeys: {
				$setUnion: [
					{
						$map: {
							input: '$allSportsPersonParticipants',
							as: 'sp',
							in: '$$sp.key',
						},
					},
					[],
				],
			},
		},
	},
];

exports.eventKeyMomentsFacet = eventKeyMomentsFacet;
