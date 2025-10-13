const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Resolves SGO memberships on a nation's sgoMemberships array into ids / keys.
 * Mirrors clubSgosFacet logic.
 */
const nationSgosFacet = [
    { $project: { sgoMemberships: 1 } },
    { $unwind: '$sgoMemberships' },
    {
        $match: {
            'sgoMemberships._externalSgoId': { $type: 'string', $ne: '' },
            'sgoMemberships._externalSgoIdScope': { $type: 'string', $ne: '' },
        },
    },
    {
        $group: {
            _id: {
                id: '$sgoMemberships._externalSgoId',
                scope: '$sgoMemberships._externalSgoIdScope',
            },
        },
    },
    {
        $project: {
            _id: 0,
            sgoId: '$_id.id',
            sgoScope: '$_id.scope',
        },
    },
    {
        $lookup: {
            from: 'sgos',
            let: { sgoId: '$sgoId', sgoScope: '$sgoScope' },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ['$_externalId', '$$sgoId'] },
                                { $eq: ['$_externalIdScope', '$$sgoScope'] },
                            ],
                        },
                    },
                },
                { $set: { key: { $concat: ['$$sgoId', keySeparator, '$$sgoScope'] } } },
                { $project: { _id: 1, key: 1 } },
            ],
            as: 'sgo',
        },
    },
    { $unwind: { path: '$sgo', preserveNullAndEmptyArrays: false } },
    {
        $group: {
            _id: null,
            ids: { $addToSet: '$sgo._id' },
            keys: { $addToSet: '$sgo.key' },
        },
    },
    { $project: { _id: 0, ids: 1, keys: 1 } },
];

////////////////////////////////////////////////////////////////////////////////
exports.nationSgosFacet = nationSgosFacet;