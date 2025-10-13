const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Finds teams referencing the nation via _externalNationId / _externalNationIdScope.
 */
const nationTeamsFacet = [
    {
        $lookup: {
            from: 'teams',
            let: { nid: '$_externalId', ns: '$_externalIdScope' },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ['$_externalNationId', '$$nid'] },
                                { $eq: ['$_externalNationIdScope', '$$ns'] },
                            ],
                        },
                    },
                },
                { $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
            ],
            as: 'teams',
        },
    },
    {
        $project: {
            ids: {
                $setUnion: [
                    { $map: { input: '$teams', as: 't', in: '$$t._id' } },
                    [],
                ],
            },
            keys: {
                $setUnion: [
                    {
                        $map: {
                            input: '$teams',
                            as: 't',
                            in: { $concat: ['$$t._externalId', keySeparator, '$$t._externalIdScope'] },
                        },
                    },
                    [],
                ],
            },
        },
    },
];

////////////////////////////////////////////////////////////////////////////////
exports.nationTeamsFacet = nationTeamsFacet;