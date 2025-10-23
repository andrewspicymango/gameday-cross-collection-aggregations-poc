//////////////////////////////////////////////////////////////////////////////
// Directed, field-labelled graph
const EDGES = {
	// competition -> sgo
	// stage -> competition
	competition: { stages: 'stage', sgos: 'sgo' },
	// stage -> competition
	// event -> stage
	// ranking -> stage
	stage: { events: 'event', competitions: 'competition', rankings: 'ranking' },
	// event -> stage
	// event -> team
	// event -> venue
	// event -> sportsPerson
	// ranking -> event
	// keyMoment -> event
	event: { teams: 'team', venues: 'venue', sportsPersons: 'sportsPerson', stages: 'stage', rankings: 'ranking', keyMoments: 'keyMoment' },
	// team -> sgo
	// team -> club
	// team -> nation
	// team -> sportsPerson (via members, deprecated)
	// team -> venue
	// ranking -> team
	// event -> team
	// staff -> team
	// keyMoment -> team
	team: {
		clubs: 'club',
		events: 'event',
		nations: 'nation',
		sportsPersons: 'sportsPerson',
		staff: 'staff',
		rankings: 'ranking',
		sgos: 'sgo',
		keyMoments: 'keyMoment',
		venues: 'venue',
	},
	// venue -> sgo
	// event -> venue
	// team -> venue
	// club -> venue
	// nation -> venue
	venue: { events: 'event', teams: 'team', sgos: 'sgo', clubs: 'club', nations: 'nation' },
	// club -> sgo
	// club -> sportsPerson (via members, deprecated)
	// club -> venue
	// team -> club
	// staff -> club
	club: { teams: 'team', sgos: 'sgo', venues: 'venue', sportsPersons: 'sportsPerson', staff: 'staff' },
	// sgo -> sgo
	// competition -> sgo
	// venue -> sgo
	// team -> sgo
	// club -> sgo
	// nation -> sgo
	sgo: { competitions: 'competition', sgos: 'sgo', venues: 'venue', clubs: 'club', nations: 'nation', teams: 'team' },
	// nation -> sgo
	// nation -> venue
	// team -> nation
	// staff -> nation
	nation: { teams: 'team', sgos: 'sgo', venues: 'venue', staff: 'staff' },
	// staff -> team
	// staff -> club
	// staff -> nation
	// staff -> sportsPerson
	staff: { sportsPersons: 'sportsPerson', teams: 'team', clubs: 'club', nations: 'nation' },
	// staff -> sportsPerson
	// team -> sportsPerson
	// club -> sportsPerson
	// ranking -> sportsPerson
	// keyMoment -> sportsPerson
	// event -> sportsPerson
	sportsPerson: { teams: 'team', clubs: 'club', events: 'event', staff: 'staff', rankings: 'ranking', keyMoments: 'keyMoment' },
	// ranking -> event
	// ranking -> stage
	// ranking -> team
	// ranking -> sportsPerson
	ranking: { events: 'event', stages: 'stage', teams: 'team', sportsPersons: 'sportsPerson' },
	// keyMoment -> event
	// keyMoment -> sportsPerson
	// keyMoment -> team
	keyMoment: { events: 'event', sportsPersons: 'sportsPerson', teams: 'team' },
};

module.exports = EDGES;
