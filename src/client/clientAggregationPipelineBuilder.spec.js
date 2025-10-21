const buildMaterialisedListsPipelineTotalMax = require('./clientAggregationPipelineBuilder');

////////////////////////////////////////////////////////////////////////////////
describe('Client Aggregation Pipeline Builder', () => {
	//////////////////////////////////////////////////////////////////////////////
	// Test Data
	//////////////////////////////////////////////////////////////////////////////
	const basicRoutes = {
		rootType: 'competition',
		rootExternalKey: '289175 @ fifa',
		totalMax: 20,
		includeTypes: ['stage', 'sgo'],
		routes: [
			{ key: 'directStages', to: 'stage', via: ['competition.stages->stage'] },
			{ key: 'directSgos', to: 'sgo', via: ['competition.sgos->sgo'] },
		],
	};
	//////////////////////////////////////////////////////////////////////////////
	const multiHopRoutes = {
		rootType: 'competition',
		rootExternalKey: '289175 @ fifa',
		totalMax: 50,
		includeTypes: ['team', 'venue', 'sportsPerson', 'staff'],
		routes: [
			{
				key: 'teamsViaStagesAndEvents',
				to: 'team',
				via: ['competition.stages->stage', 'stage.events->event', 'event.teams->team'],
			},
			{
				key: 'venuesViaStagesAndEvents',
				to: 'venue',
				via: ['competition.stages->stage', 'stage.events->event', 'event.venues->venue'],
			},
			{
				key: 'sportsPersonsViaEvents',
				to: 'sportsPerson',
				via: ['competition.stages->stage', 'stage.events->event', 'event.sportsPersons->sportsPerson'],
			},
			{
				key: 'staffViaTeams',
				to: 'staff',
				via: ['competition.stages->stage', 'stage.events->event', 'event.teams->team', 'team.staff->staff'],
			},
		],
	};
	//////////////////////////////////////////////////////////////////////////////
	const sharedHopRoutes = {
		rootType: 'competition',
		rootExternalKey: '289175 @ fifa',
		totalMax: 30,
		includeTypes: ['team', 'venue', 'ranking'],
		routes: [
			{
				key: 'teamsViaEvents',
				to: 'team',
				via: ['competition.stages->stage', 'stage.events->event', 'event.teams->team'],
			},
			{
				key: 'venuesViaEvents',
				to: 'venue',
				via: ['competition.stages->stage', 'stage.events->event', 'event.venues->venue'],
			},
			{
				key: 'rankingsViaEvents',
				to: 'ranking',
				via: ['competition.stages->stage', 'stage.events->event', 'event.rankings->ranking'],
			},
			{
				key: 'rankingsViaStages',
				to: 'ranking',
				via: ['competition.stages->stage', 'stage.rankings->ranking'],
			},
		],
	};
	//////////////////////////////////////////////////////////////////////////////
	const unionRoutes = {
		rootType: 'competition',
		rootExternalKey: '289175 @ fifa',
		totalMax: 25,
		includeTypes: ['team', 'sgo'],
		routes: [
			{
				key: 'teamsViaEvents',
				to: 'team',
				via: ['competition.stages->stage', 'stage.events->event', 'event.teams->team'],
			},
			{
				key: 'teamsViaSgos',
				to: 'team',
				via: ['competition.sgos->sgo', 'sgo.teams->team'],
			},
			{
				key: 'sgosFromCompetition',
				to: 'sgo',
				via: ['competition.sgos->sgo'],
			},
			{
				key: 'sgosViaTeams',
				to: 'sgo',
				via: ['competition.stages->stage', 'stage.events->event', 'event.teams->team', 'team.sgos->sgo'],
			},
		],
	};
	//////////////////////////////////////////////////////////////////////////////
	const unionRoutesWithMax4 = {
		rootType: 'competition',
		rootExternalKey: '289175 @ fifa',
		totalMax: 25,
		includeTypes: ['team', 'sgo'],
		routes: [
			{
				key: 'teamsViaEvents',
				to: 'team',
				via: ['competition.stages->stage', 'stage.events->event', 'event.teams->team'],
			},
			{
				key: 'teamsViaSgos',
				to: 'team',
				via: ['competition.sgos->sgo', 'sgo.teams->team'],
			},
			{
				key: 'sgosFromCompetition',
				to: 'sgo',
				via: ['competition.sgos->sgo'],
			},
			{
				key: 'sgosViaTeams',
				to: 'sgo',
				via: ['competition.stages->stage', 'stage.events->event', 'event.teams->team', 'team.sgos->sgo'],
			},
		],
	};
	//////////////////////////////////////////////////////////////////////////////
	const budgetTestRoutes = {
		rootType: 'competition',
		rootExternalKey: '289175 @ fifa',
		totalMax: 5, // Intentionally small to test overflow
		includeTypes: ['stage', 'event', 'team', 'venue'],
		routes: [
			{
				key: 'stages',
				to: 'stage',
				via: ['competition.stages->stage'],
			},
			{
				key: 'events',
				to: 'event',
				via: ['competition.stages->stage', 'stage.events->event'],
			},
			{
				key: 'teams',
				to: 'team',
				via: ['competition.stages->stage', 'stage.events->event', 'event.teams->team'],
			},
			{
				key: 'venues',
				to: 'venue',
				via: ['competition.stages->stage', 'stage.events->event', 'event.venues->venue'],
			},
		],
	};
	//////////////////////////////////////////////////////////////////////////////
	const teamRootRoutes = {
		rootType: 'team',
		rootExternalKey: 'team123 @ fifa',
		totalMax: 15,
		includeTypes: ['event', 'club', 'sportsPerson', 'venue'],
		routes: [
			{
				key: 'eventsForTeam',
				to: 'event',
				via: ['team.events->event'],
			},
			{
				key: 'clubForTeam',
				to: 'club',
				via: ['team.clubs->club'],
			},
			{
				key: 'sportsPersonsForTeam',
				to: 'sportsPerson',
				via: ['team.sportsPersons->sportsPerson'],
			},
			{
				key: 'venuesViaEvents',
				to: 'venue',
				via: ['team.events->event', 'event.venues->venue'],
			},
		],
	};
	////////////////////////////////////////////////////////////////////////////////
	const deepRoutes = {
		rootType: 'competition',
		rootExternalKey: '289175 @ fifa',
		totalMax: 40,
		includeTypes: ['club', 'nation', 'keyMoment'],
		routes: [
			{
				key: 'clubsViaTeams',
				to: 'club',
				via: ['competition.stages->stage', 'stage.events->event', 'event.teams->team', 'team.clubs->club'],
			},
			{
				key: 'nationsViaTeams',
				to: 'nation',
				via: ['competition.stages->stage', 'stage.events->event', 'event.teams->team', 'team.nations->nation'],
			},
			{
				key: 'keyMomentsViaEvents',
				to: 'keyMoment',
				via: ['competition.stages->stage', 'stage.events->event', 'event.keyMoments->keyMoment'],
			},
		],
	};

	////////////////////////////////////////////////////////////////////////////////
	// Successful Test Cases
	////////////////////////////////////////////////////////////////////////////////

	//////////////////////////////////////////////////////////////////////////////
	describe('Successful Pipeline Generation', () => {
		////////////////////////////////////////////////////////////////////////////
		test('should generate pipeline for basic single-hop routes', () => {
			const pipeline = buildMaterialisedListsPipelineTotalMax(basicRoutes);
			expect(pipeline).toBeDefined();
			expect(Array.isArray(pipeline)).toBe(true);
			expect(pipeline.length).toBeGreaterThan(0);
			// Check for facet stage
			const facetStage = pipeline.find((stage) => stage.$facet);
			expect(facetStage).toBeDefined();
			expect(facetStage.$facet).toHaveProperty('stage');
			expect(facetStage.$facet).toHaveProperty('sgo');
		});
		////////////////////////////////////////////////////////////////////////////
		test('should generate pipeline for multi-hop routes', () => {
			const pipeline = buildMaterialisedListsPipelineTotalMax(multiHopRoutes);
			expect(pipeline).toBeDefined();
			expect(Array.isArray(pipeline)).toBe(true);
			expect(pipeline.length).toBeGreaterThan(0);
			// Check for all expected route keys
			const facetStage = pipeline.find((stage) => stage.$facet);
			expect(facetStage).toBeDefined();
			expect(facetStage.$facet).toHaveProperty('competition');
			expect(facetStage.$facet).toHaveProperty('sportsPerson');
			expect(facetStage.$facet).toHaveProperty('staff');
			expect(facetStage.$facet).toHaveProperty('team');
			expect(facetStage.$facet).toHaveProperty('venue');
		});
		////////////////////////////////////////////////////////////////////////////
		test('should optimize shared hop routes', () => {
			const pipeline = buildMaterialisedListsPipelineTotalMax(sharedHopRoutes);
			expect(pipeline).toBeDefined();
			expect(Array.isArray(pipeline)).toBe(true);
			expect(pipeline.length).toEqual(27);
			const facetStage = pipeline.find((stage) => stage.$facet);
			expect(facetStage).toBeDefined();
			expect(facetStage.$facet).toHaveProperty('team');
			expect(facetStage.$facet).toHaveProperty('venue');
			expect(facetStage.$facet).toHaveProperty('ranking');
		});
		////////////////////////////////////////////////////////////////////////////
		test('should handle multiple routes to same type (union)', () => {
			const pipeline = buildMaterialisedListsPipelineTotalMax(unionRoutes);
			expect(pipeline).toBeDefined();
			expect(Array.isArray(pipeline)).toBe(true);
			expect(pipeline.length).toEqual(24);
			const facetStage = pipeline.find((stage) => stage.$facet);
			expect(facetStage).toBeDefined();
			expect(facetStage.$facet).toHaveProperty('team');
			expect(facetStage.$facet).toHaveProperty('sgo');
		});
		////////////////////////////////////////////////////////////////////////////
		test('should handle budget constraints', () => {
			const pipeline = buildMaterialisedListsPipelineTotalMax(budgetTestRoutes);
			expect(pipeline).toBeDefined();
			expect(Array.isArray(pipeline)).toBe(true);
			// Should still generate a valid pipeline even with small budget
			const facetStage = pipeline.find((stage) => stage.$facet);
			expect(facetStage).toBeDefined();
		});
		////////////////////////////////////////////////////////////////////////////
		test('should work with different root types', () => {
			const pipeline = buildMaterialisedListsPipelineTotalMax(teamRootRoutes);
			expect(pipeline).toBeDefined();
			expect(Array.isArray(pipeline)).toBe(true);
			const facetStage = pipeline.find((stage) => stage.$facet);
			expect(facetStage).toBeDefined();
			expect(facetStage.$facet).toHaveProperty('event');
			expect(facetStage.$facet).toHaveProperty('club');
			expect(facetStage.$facet).toHaveProperty('sportsPerson');
			expect(facetStage.$facet).toHaveProperty('venue');
		});
		////////////////////////////////////////////////////////////////////////////
		test('should handle deep route traversals', () => {
			const pipeline = buildMaterialisedListsPipelineTotalMax(deepRoutes);
			expect(pipeline).toBeDefined();
			expect(Array.isArray(pipeline)).toBe(true);
			const facetStage = pipeline.find((stage) => stage.$facet);
			expect(facetStage).toBeDefined();
			expect(facetStage.$facet).toHaveProperty('club');
			expect(facetStage.$facet).toHaveProperty('nation');
			expect(facetStage.$facet).toHaveProperty('keyMoment');
		});
	});

	////////////////////////////////////////////////////////////////////////////////
	// Error Test Cases
	////////////////////////////////////////////////////////////////////////////////

	//////////////////////////////////////////////////////////////////////////////
	describe('Error Handling', () => {
		////////////////////////////////////////////////////////////////////////////
		test('should throw error for non-contiguous route', () => {
			const nonContiguousConfig = {
				rootType: 'competition',
				rootExternalKey: 'test',
				totalMax: 10,
				includeTypes: ['team'],
				routes: [
					{
						key: 'badRoute',
						to: 'team',
						via: ['competition.stages->stage', 'event.teams->team'], // Missing stage.events->event
					},
				],
			};
			expect(() => {
				buildMaterialisedListsPipelineTotalMax(nonContiguousConfig);
			}).toThrow();
		});
		////////////////////////////////////////////////////////////////////////////
		test('should throw error for invalid field', () => {
			const invalidFieldConfig = {
				rootType: 'competition',
				rootExternalKey: 'test',
				totalMax: 10,
				includeTypes: ['stage'],
				routes: [
					{
						key: 'badField',
						to: 'stage',
						via: ['competition.nonexistent->stage'],
					},
				],
			};
			expect(() => {
				buildMaterialisedListsPipelineTotalMax(invalidFieldConfig);
			}).toThrow();
		});
		////////////////////////////////////////////////////////////////////////////
		test('should throw error for cyclic routes', () => {
			const cyclicConfig = {
				rootType: 'sgo',
				rootExternalKey: 'test',
				totalMax: 10,
				includeTypes: ['sgo'],
				routes: [
					{
						key: 'cyclicRoute',
						to: 'sgo',
						via: ['sgo.sgos->sgo', 'sgo.sgos->sgo'], // This creates a cycle back to sgo type
					},
				],
			};
			expect(() => {
				buildMaterialisedListsPipelineTotalMax(cyclicConfig);
			}).toThrow();
		});
		////////////////////////////////////////////////////////////////////////////
		test('should throw error for wrong final destination', () => {
			const wrongDestinationConfig = {
				rootType: 'competition',
				rootExternalKey: 'test',
				totalMax: 10,
				includeTypes: ['team'],
				routes: [
					{
						key: 'wrongDestination',
						to: 'team',
						via: ['competition.stages->stage'], // Ends at stage, not team
					},
				],
			};

			expect(() => {
				buildMaterialisedListsPipelineTotalMax(wrongDestinationConfig);
			}).toThrow();
		});
		////////////////////////////////////////////////////////////////////////////
		test('should throw error for missing required parameters', () => {
			const incompleteConfig = {
				rootType: 'competition',
				// Missing rootExternalKey
				totalMax: 10,
				includeTypes: ['stage'],
				routes: [],
			};

			expect(() => {
				buildMaterialisedListsPipelineTotalMax(incompleteConfig);
			}).toThrow();
		});
		////////////////////////////////////////////////////////////////////////////
		test('should throw error for invalid route structure', () => {
			const invalidRouteConfig = {
				rootType: 'competition',
				rootExternalKey: 'test',
				totalMax: 10,
				includeTypes: ['stage'],
				routes: [
					{
						// Missing 'key' property
						to: 'stage',
						via: ['competition.stages->stage'],
					},
				],
			};

			expect(() => {
				buildMaterialisedListsPipelineTotalMax(invalidRouteConfig);
			}).toThrow();
		});
		////////////////////////////////////////////////////////////////////////////
		test('should throw error for empty routes array', () => {
			const emptyRoutesConfig = {
				rootType: 'competition',
				rootExternalKey: 'test',
				totalMax: 10,
				includeTypes: [],
				routes: [],
			};
			expect(() => {
				buildMaterialisedListsPipelineTotalMax(emptyRoutesConfig);
			}).toThrow();
		});
	});

	////////////////////////////////////////////////////////////////////////////////
	// Pipeline Structure Tests
	////////////////////////////////////////////////////////////////////////////////

	//////////////////////////////////////////////////////////////////////////////
	describe('Pipeline Structure Validation', () => {
		////////////////////////////////////////////////////////////////////////////
		test('should contain required pipeline stages', () => {
			const pipeline = buildMaterialisedListsPipelineTotalMax(basicRoutes);
			// Should have at least a match stage and facet stage
			const matchStage = pipeline.find((stage) => stage.$match);
			const facetStage = pipeline.find((stage) => stage.$facet);
			expect(matchStage).toBeDefined();
			expect(facetStage).toBeDefined();
		});
		////////////////////////////////////////////////////////////////////////////
		test('should have correct match criteria', () => {
			const pipeline = buildMaterialisedListsPipelineTotalMax(basicRoutes);
			const matchStage = pipeline.find((stage) => stage.$match);
			expect(matchStage.$match).toHaveProperty('resourceType');
			expect(matchStage.$match.resourceType).toBe('competition');
			expect(matchStage.$match).toHaveProperty('externalKey');
			expect(matchStage.$match.externalKey).toBe('289175 @ fifa');
		});
		////////////////////////////////////////////////////////////////////////////
		test('should generate valid MongoDB aggregation pipeline', () => {
			const pipeline = buildMaterialisedListsPipelineTotalMax(multiHopRoutes);
			// Each stage should be a valid MongoDB operation
			pipeline.forEach((stage) => {
				expect(typeof stage).toBe('object');
				expect(stage).not.toBeNull();
				// Should have exactly one MongoDB operation per stage
				const operations = Object.keys(stage).filter((key) => key.startsWith('$'));
				expect(operations.length).toBeGreaterThanOrEqual(1);
			});
		});
	});

	////////////////////////////////////////////////////////////////////////////////
	// Performance and Edge Case Tests
	////////////////////////////////////////////////////////////////////////////////

	//////////////////////////////////////////////////////////////////////////////
	describe('Edge Cases and Performance', () => {
		////////////////////////////////////////////////////////////////////////////
		test('should handle zero budget', () => {
			const zeroBudgetConfig = {
				rootType: 'competition',
				rootExternalKey: 'test',
				totalMax: 0,
				includeTypes: ['stage'],
				routes: [
					{
						key: 'stages',
						to: 'stage',
						via: ['competition.stages->stage'],
					},
				],
			};

			const pipeline = buildMaterialisedListsPipelineTotalMax(zeroBudgetConfig);
			expect(pipeline).toBeDefined();
			expect(Array.isArray(pipeline)).toBe(true);
		});
		////////////////////////////////////////////////////////////////////////////
		test('should handle large number of routes', () => {
			const manyRoutesConfig = {
				rootType: 'competition',
				rootExternalKey: 'test',
				totalMax: 100,
				includeTypes: ['stage', 'event', 'team'],
				routes: Array(1000)
					.fill(null)
					.map((_, i) => ({
						key: `route${i}`,
						to: 'stage',
						via: ['competition.stages->stage'],
					})),
			};

			expect(() => {
				const pipeline = buildMaterialisedListsPipelineTotalMax(manyRoutesConfig);
				expect(pipeline).toBeDefined();
			}).not.toThrow();
		});
	});
});
