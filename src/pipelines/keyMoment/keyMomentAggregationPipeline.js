const { keyMomentMetaFacet } = require('./keyMomentMetaFacet.js');
const { keyMomentEventsFacet } = require('./keyMomentEventsFacet.js');
const { keyMomentTeamsFacet } = require('./keyMomentTeamsFacet.js');
const { keyMomentSportsPersonsFacet } = require('./keyMomentSportsPersonsFacet.js');

const { keySeparator } = require('../constants');
const { keyInAggregation } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const pipeline = (config, EVENT_SCOPE, EVENT_ID, TYPE, SUBTYPE, DATETIME) => {
	if (!DATETIME || isNaN(new Date(DATETIME).getTime())) {
		throw new Error('Invalid DATETIME parameter: must be a valid date string');
	}
	return [
		{ $match: { _externalEventId: EVENT_ID, _externalEventIdScope: EVENT_SCOPE, type: TYPE, subType: SUBTYPE, dateTime: new Date(DATETIME) } },
		//////////////////////////////////////////////////////////////////////////////
		{
			$facet: {
				meta: keyMomentMetaFacet,
				events: keyMomentEventsFacet,
				teams: keyMomentTeamsFacet,
				sportsPersons: keyMomentSportsPersonsFacet,
			},
		},
		//////////////////////////////////////////////////////////////////////////////
		{
			$project: {
				resourceType: { $first: '$meta.resourceType' },
				externalKey: {
					$concat: [
						{
							$dateToString: {
								date: { $first: '$meta.keyMomentDateTime' },
								format: '%Y-%m-%dT%H:%M:%S.%LZ', // ISO format
							},
						},
						keySeparator,
						{ $first: '$meta.keyMomentEventId' },
						keySeparator,
						{ $first: '$meta.keyMomentEventIdScope' },
						keySeparator,
						{ $first: '$meta.keyMomentType' },
						keySeparator,
						{ $first: '$meta.keyMomentSubType' },
					],
				},
				gamedayId: { $first: '$meta._id' },
				_externalEventId: { $first: '$meta.keyMomentEventId' },
				_externalEventIdScope: { $first: '$meta.keyMomentEventIdScope' },
				type: { $first: '$meta.keyMomentType' },
				subType: { $first: '$meta.keyMomentSubType' },
				dateTime: { $first: '$meta.keyMomentDateTime' },
				name: { $first: '$meta.name' },
				events: { $ifNull: [{ $first: '$events.ids' }, []] },
				eventKeys: { $ifNull: [{ $first: '$events.keys' }, []] },
				teams: { $ifNull: [{ $first: '$teams.ids' }, []] },
				teamKeys: { $ifNull: [{ $first: '$teams.keys' }, []] },
				sportsPersons: { $ifNull: [{ $first: '$sportsPersons.ids' }, []] },
				sportsPersonKeys: { $ifNull: [{ $first: '$sportsPersons.keys' }, []] },
			},
		},
		{
			$addFields: {
				resourceType: { $toLower: '$resourceType' },
				externalKey: '$externalKey',
				gamedayId: '$gamedayId',
				_externalEventId: '$_externalEventId',
				_externalEventIdScope: '$_externalEventIdScope',
				type: '$type',
				subType: '$subType',
				dateTime: '$dateTime',
				name: '$name',
				lastUpdated: '$$NOW',
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
};

////////////////////////////////////////////////////////////////////////////////
function queryForKeyMomentAggregationDoc(eventId, eventIdScope, type, subType, dateTime) {
	if (!eventId || !eventIdScope) throw new Error('Invalid parameters: eventId and eventIdScope are required for a keyMoment');
	if (!type || !subType) throw new Error('Invalid parameters: type and subType are required for a keyMoment');
	if (!dateTime || isNaN(new Date(dateTime).getTime())) throw new Error('Invalid parameters: dateTime must be a valid date string for a keyMoment');

	// The `toISOString()` method always returns dates in UTC timezone with the format `YYYY-MM-DDTHH:mm:ss.sssZ`, where the `Z` indicates UTC (Zulu time).
	// For example:
	// - `new Date('2023-12-25T15:30:45.123').toISOString()` returns `"2023-12-25T15:30:45.123Z"`
	// - The `Z` suffix explicitly indicates the time is in UTC
	// - It always uses 3-digit milliseconds (padding with zeros if needed)
	// - No timezone offset is ever included - it's always UTC
	// This is different from timezone-aware formats that might show `+00:00` or other offsets.
	const dateTimeIso = new Date(dateTime).toISOString(); // e.g. "2023-12-25T15:30:45.123Z"
	return {
		resourceType: 'keyMoment'.toLowerCase(),
		externalKey: `${dateTimeIso}${keySeparator}${eventId}${keySeparator}${eventIdScope}${keySeparator}${type}${keySeparator}${subType}`,
	};
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, queryForKeyMomentAggregationDoc };
