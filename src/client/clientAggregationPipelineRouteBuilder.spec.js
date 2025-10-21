const { clientAggregationPipelineRouteBuilder } = require('./clientAggregationPipelineRouteBuilder');
const EDGES = require('./clientAggregationPipelineBuilderEdges');

////////////////////////////////////////////////////////////////////////////////
function sig(r) {
	return `${r.to}|${r.via.join('~')}`;
}

////////////////////////////////////////////////////////////////////////////////
function hasRoute(routes, to, viaArr) {
	const s = `${to}|${viaArr.join('~')}`;
	return routes.some((r) => sig(r) === s);
}

////////////////////////////////////////////////////////////////////////////////
describe('clientAggregationPipelineRouteBuilder', () => {
	//////////////////////////////////////////////////////////////////////////////
	test('produces teams via events and via sgos from unordered edges', () => {
		////////////////////////////////////////////////////////////////////////////
		// intentionally unordered
		const edgeIds = ['event.team', 'c.stage', 'stage.event', 'competition.sgo', 'o.t'];
		const routes = clientAggregationPipelineRouteBuilder({ rootType: 'competition', includeTypes: ['team'], edgeIds });
		////////////////////////////////////////////////////////////////////////////
		// Expected two routes
		expect(routes.length).toBe(2);
		////////////////////////////////////////////////////////////////////////////
		// teams via events
		expect(hasRoute(routes, 'team', ['competition.stages->stage', 'stage.events->event', 'event.teams->team'])).toBe(true);
		////////////////////////////////////////////////////////////////////////////
		// teams via sgos
		expect(hasRoute(routes, 'team', ['competition.sgos->sgo', 'sgo.teams->team'])).toBe(true);
		////////////////////////////////////////////////////////////////////////////
		// keys must exist and be non-empty strings
		routes.forEach((r) => {
			expect(typeof r.key).toBe('string');
			expect(r.key.length).toBeGreaterThan(0);
		});
	});
	//////////////////////////////////////////////////////////////////////////////
	test('silently ignores unreachable edges from the root', () => {
		const edgeIds = [
			'team.club', // unreachable because we didn't include competition→...→team
			'competition.stage', // Not interested in materialising stages
			'stage.event',
			// no event.teams->team edge, so nothing reaches team either
		];
		const routes = clientAggregationPipelineRouteBuilder({ rootType: 'competition', includeTypes: ['team', 'club'], edgeIds });
		////////////////////////////////////////////////////////////////////////////
		// No routes should reach team or club given the missing edges
		expect(routes).toEqual([]);
	});
	//////////////////////////////////////////////////////////////////////////////
	test('filters by includeTypes (only returns routes whose terminal type is included)', () => {
		const edgeIds = ['c.s', 'stage.event', 'event.venue', 'event.t'];
		const onlyTeams = clientAggregationPipelineRouteBuilder({ rootType: 'competition', includeTypes: ['team'], edgeIds });
		expect(onlyTeams.length).toBe(1);
		expect(hasRoute(onlyTeams, 'team', ['competition.stages->stage', 'stage.events->event', 'event.teams->team'])).toBe(true);
		const onlyVenues = clientAggregationPipelineRouteBuilder({ rootType: 'competition', includeTypes: ['venue'], edgeIds });
		expect(onlyVenues.length).toBe(1);
		expect(hasRoute(onlyVenues, 'venue', ['competition.stages->stage', 'stage.events->event', 'event.venues->venue'])).toBe(true);
	});
	//////////////////////////////////////////////////////////////////////////////
	test('prevents cycles (no stage→event→stage ping-pong)', () => {
		const edgeIds = [
			'competition.stage',
			's.event',
			'event.stage', // would create cycle if allowed
			'e.team',
		];
		const routes = clientAggregationPipelineRouteBuilder({ rootType: 'competition', includeTypes: ['team', 'stage'], edgeIds });
		////////////////////////////////////////////////////////////////////////////
		// We should have team via events, but not any route that re-visits 'stage' after 'event'
		expect(hasRoute(routes, 'team', ['competition.stages->stage', 'stage.events->event', 'event.teams->team'])).toBe(true);
		////////////////////////////////////////////////////////////////////////////
		// No route whose via includes ... stage.events->event ~ event.stages->stage (cycle)
		const anyCycle = routes.some((r) => r.via.join('~').includes('stage.events->event~event.stages->stage'));
		expect(anyCycle).toBe(false);
	});
	//////////////////////////////////////////////////////////////////////////////
	test('deduplicates routes when duplicate edges are supplied', () => {
		const edgeIds = [
			'competition.stage',
			'competition.stage', // duplicate
			'stage.event',
			'event.team',
			'event.team', // duplicate
		];
		const routes = clientAggregationPipelineRouteBuilder({ rootType: 'competition', includeTypes: ['team'], edgeIds });
		// Only one team route expected
		expect(routes.length).toBe(1);
		expect(hasRoute(routes, 'team', ['competition.stages->stage', 'stage.events->event', 'event.teams->team'])).toBe(true);
	});
	//////////////////////////////////////////////////////////////////////////////
	test('respects maxDepth guardrail (cuts off deeper paths)', () => {
		// Build a 3-hop path; with maxDepth=2 we should not get the 3rd hop route
		const edgeIds = [
			'competition.stage',
			'stage.event',
			'event.team', // 3rd hop
		];
		const routesDepth2 = clientAggregationPipelineRouteBuilder({
			rootType: 'competition',
			includeTypes: ['team'],
			edgeIds,
			EDGES: null,
			maxDepth: 2, // allow only 2 hops max
		});
		// With depth=2, we cannot reach team (requires 3 hops). No routes.
		expect(routesDepth2).toEqual([]);
		const routesDepth3 = clientAggregationPipelineRouteBuilder({ rootType: 'competition', includeTypes: ['team'], edgeIds, EDGES: null, maxDepth: 3 });
		expect(routesDepth3.length).toBe(1);
	});
	//////////////////////////////////////////////////////////////////////////////
	test('respects maxRoutes guardrail (caps number of routes)', () => {
		// Create branching: competition->stages->event -> teams AND venues
		const edgeIds = ['competition.stage', 'stage.event', 'event.team', 'e.v'];
		const routesCap1 = clientAggregationPipelineRouteBuilder({ rootType: 'competition', includeTypes: ['team', 'venue'], edgeIds, EDGES: null, maxRoutes: 1 });
		expect(routesCap1.length).toBe(1);
		const routesCap2 = clientAggregationPipelineRouteBuilder({ rootType: 'competition', includeTypes: ['team', 'venue'], edgeIds, EDGES: null, maxRoutes: 2 });
		expect(routesCap2.length).toBe(2);
	});
	//////////////////////////////////////////////////////////////////////////////
	test('keys are deterministic but we only assert they are present and unique per signature', () => {
		const edgeIds = ['competition.stage', 'stage.event', 'event.team'];
		const routes1 = clientAggregationPipelineRouteBuilder({ rootType: 'competition', includeTypes: ['team'], edgeIds });
		const routes2 = clientAggregationPipelineRouteBuilder({ rootType: 'competition', includeTypes: ['team'], edgeIds });
		expect(routes1.length).toBe(1);
		expect(routes2.length).toBe(1);
		expect(routes1[0].key).toEqual(routes2[0].key); // deterministic for same inputs
		expect(typeof routes1[0].key).toBe('string');
		expect(routes1[0].key.length).toBeGreaterThan(0);
	});
});
