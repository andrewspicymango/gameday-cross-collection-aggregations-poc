const { keySeparator, clubSeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * MongoDB aggregation pipeline facet for retrieving staff documents associated with a club.
 *
 * Performs a lookup on the 'staff' collection matching by external club ID and scope,
 * then constructs a staffKey for each staff member by concatenating their external
 * sports person ID, scope, club ID, and scope with separators.
 *
 * @type {Array<Object>} MongoDB aggregation pipeline stages
 *
 * @returns {Object} Facet result containing:
 * @returns {Array<ObjectId>} returns.ids - Array of staff document IDs
 * @returns {Object} returns.keys - Object mapping staffKey strings to staff document IDs
 *
 * @example
 * // Usage in aggregation pipeline
 * db.clubs.aggregate([{ $facet: { staff: clubStaffFacet } }])
 */
const clubStaffFacet = [
	{
		$lookup: {
			from: 'staff',
			let: { thisClubId: '$_externalId', thisClubIdScope: '$_externalIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalClubId', '$$thisClubId'] }, { $eq: ['$_externalClubIdScope', '$$thisClubIdScope'] }] } } },
				{
					$project: {
						_id: 1,
						staffKey: {
							$cond: [
								{
									$and: [
										{ $eq: [{ $type: '$_externalTeamId' }, 'string'] },
										{ $ne: ['$_externalTeamId', ''] },
										{ $eq: [{ $type: '$_externalTeamIdScope' }, 'string'] },
										{ $ne: ['$_externalTeamIdScope', ''] },
										{ $eq: [{ $type: '$_externalSportsPersonId' }, 'string'] },
										{ $ne: ['$_externalSportsPersonId', ''] },
										{ $eq: [{ $type: '$_externalSportsPersonIdScope' }, 'string'] },
										{ $ne: ['$_externalSportsPersonIdScope', ''] },
									],
								},
								{
									$concat: [
										'$_externalSportsPersonId',
										keySeparator,
										'$_externalSportsPersonIdScope',
										clubSeparator,
										'$_externalClubId',
										keySeparator,
										'$_externalClubIdScope',
									],
								},
								null,
							],
						},
					},
				},
				{ $match: { staffKey: { $ne: null } } },
			],
			as: 'staffDocs',
		},
	},
	{
		$project: {
			_id: 0,
			ids: { $setUnion: [{ $map: { input: '$staffDocs', as: 's', in: '$$s._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$staffDocs' }, 0] }, { $arrayToObject: { $map: { input: '$staffDocs', as: 's', in: ['$$s.staffKey', '$$s._id'] } } }, {}] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.clubStaffFacet = clubStaffFacet;
