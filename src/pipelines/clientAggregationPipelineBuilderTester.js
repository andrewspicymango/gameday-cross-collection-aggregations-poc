////////////////////////////////////////////////////////////////////////////////
// Test Cases for Pipeline Builder
////////////////////////////////////////////////////////////////////////////////

const { buildMaterialisedListsPipelineTotalMax } = require('./clientAggregationPipelineBuilder');

////////////////////////////////////////////////////////////////////////////////
// 1. BASIC FUNCTIONALITY - Simple single-hop routes
const basicRoutes = {
	rootType: 'competition',
	rootExternalKey: '289175 @ fifa',
	totalMax: 20,
	includeTypes: ['stage', 'sgo'],
	routes: [
		{
			key: 'directStages',
			to: 'stage',
			via: ['competition.stages->stage'],
		},
		{
			key: 'directSgos',
			to: 'sgo',
			via: ['competition.sgos->sgo'],
		},
	],
};

////////////////////////////////////////////////////////////////////////////////
// 2. MULTI-HOP ROUTES - Test complex traversals
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

////////////////////////////////////////////////////////////////////////////////
// 3. SHARED HOP OPTIMIZATION - Multiple routes sharing common paths
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

////////////////////////////////////////////////////////////////////////////////
// 4. MULTIPLE ROUTES TO SAME TYPE - Union testing
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

////////////////////////////////////////////////////////////////////////////////
// 5. BUDGET TESTING - Test overflow handling
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

////////////////////////////////////////////////////////////////////////////////
// 6. DIFFERENT ROOT TYPES - Test from different starting points
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
// 7. COMPLEX DEEP ROUTES - Test longer paths
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
// 8. ERROR TESTING - These should throw specific errors
const errorTestCases = [
	// Non-contiguous route
	{
		name: 'Non-contiguous route',
		config: {
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
		},
	},
	// Invalid field
	{
		name: 'Invalid field',
		config: {
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
		},
	},
	// Cycle detection
	{
		name: 'Cycle detection',
		config: {
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
		},
	},
	// Wrong final destination
	{
		name: 'Wrong final destination',
		config: {
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
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
// Test runner function
////////////////////////////////////////////////////////////////////////////////
function runTests() {
	const testCases = [
		{ name: 'Basic Routes', config: basicRoutes },
		{ name: 'Multi-hop Routes', config: multiHopRoutes },
		{ name: 'Shared Hop Routes', config: sharedHopRoutes },
		{ name: 'Union Routes', config: unionRoutes },
		{ name: 'Budget Test Routes', config: budgetTestRoutes },
		{ name: 'Team Root Routes', config: teamRootRoutes },
		{ name: 'Deep Routes', config: deepRoutes },
	];
	//////////////////////////////////////////////////////////////////////////////
	console.log('='.repeat(80));
	console.log('TESTING PIPELINE BUILDER');
	console.log('='.repeat(80));

	//////////////////////////////////////////////////////////////////////////////
	// Test successful cases
	testCases.forEach(({ name, config }) => {
		console.log(`\n--- Testing: ${name} ---`);
		try {
			const pipeline = buildMaterialisedListsPipelineTotalMax(config);
			console.log(JSON.stringify(config, null, 2));
			console.log(`✅ SUCCESS: Generated ${pipeline.length} pipeline stages`);
			//////////////////////////////////////////////////////////////////////////
			// Log some key details
			const facetStage = pipeline.find((stage) => stage.$facet);
			if (facetStage) {
				const facetKeys = Object.keys(facetStage.$facet);
				console.log(`   Materializing: ${facetKeys.join(', ')}`);
			}
		} catch (error) {
			console.log(`❌ FAILED: ${error.message}`);
		}
	});

	//////////////////////////////////////////////////////////////////////////////
	// Test error cases
	console.log('\n' + '='.repeat(50));
	console.log('TESTING ERROR CASES');
	console.log('='.repeat(50));

	errorTestCases.forEach(({ name, config }) => {
		console.log(`\n--- Testing: ${name} ---`);
		try {
			const pipeline = buildMaterialisedListsPipelineTotalMax(config);
			console.log(`❌ SHOULD HAVE FAILED: Generated ${pipeline.length} stages unexpectedly`);
		} catch (error) {
			console.log(`✅ CORRECTLY FAILED: ${error.message}`);
		}
	});
}

////////////////////////////////////////////////////////////////////////////////
runTests();
