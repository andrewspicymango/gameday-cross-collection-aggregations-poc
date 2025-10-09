const { keySeparator } = require('../constants');

const eventSgoFacet = [
    // First get the stage info
    {
        $lookup: {
            from: 'stages',
            let: { sid: '$_externalStageId', ss: '$_externalStageIdScope' },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [{ $eq: ['$_externalId', '$$sid'] }, { $eq: ['$_externalIdScope', '$$ss'] }],
                        },
                    },
                },
                { $project: { _externalCompetitionId: 1, _externalCompetitionIdScope: 1 } },
            ],
            as: 'stageInfo',
        },
    },
    { $unwind: { path: '$stageInfo', preserveNullAndEmptyArrays: true } },
    // Then get the competition and its SGO memberships
    {
        $lookup: {
            from: 'competitions',
            let: { cid: '$stageInfo._externalCompetitionId', cs: '$stageInfo._externalCompetitionIdScope' },
            pipeline: [
                { $match: { $expr: { $and: [{ $eq: ['$_externalId', '$$cid'] }, { $eq: ['$_externalIdScope', '$$cs'] }] } } },
                { $project: { _id: 0, sgoMemberships: { $ifNull: ['$sgoMemberships', []] } } },
                {
                    $project: {
                        pairs: {
                            $setUnion: [
                                {
                                    $map: {
                                        input: '$sgoMemberships',
                                        as: 'm',
                                        in: {
                                            $cond: [
                                                {
                                                    $and: [
                                                        { $eq: [{ $type: '$$m._externalSgoId' }, 'string'] },
                                                        { $ne: ['$$m._externalSgoId', ''] },
                                                        { $eq: [{ $type: '$$m._externalSgoIdScope' }, 'string'] },
                                                        { $ne: ['$$m._externalSgoIdScope', ''] },
                                                    ],
                                                },
                                                { id: '$$m._externalSgoId', scope: '$$m._externalSgoIdScope', key: { $concat: ['$$m._externalSgoId', keySeparator, '$$m._externalSgoIdScope'] } },
                                                null,
                                            ],
                                        },
                                    },
                                },
                                [],
                            ],
                        },
                    },
                },
                { $project: { pairs: { $filter: { input: '$pairs', as: 'p', cond: { $ne: ['$$p', null] } } } } },
                { $limit: 1 },
            ],
            as: 'compAgg',
        },
    },
    { $project: { sgoPairs: { $ifNull: [{ $getField: { field: 'pairs', input: { $first: '$compAgg' } } }, []] } } },
    {
        $lookup: {
            from: 'sgos',
            let: { sgoKeys: { $setUnion: [{ $map: { input: '$sgoPairs', as: 'p', in: '$$p.key' } }, []] } },
            pipeline: [
                { $match: { $expr: { $and: [{ $gt: [{ $size: '$$sgoKeys' }, 0] }, { $in: [{ $concat: ['$_externalId', keySeparator, '$_externalIdScope'] }, '$$sgoKeys'] }] } } },
                { $project: { _id: 1, key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
            ],
            as: 'sgoHits',
        },
    },
    {
        $project: {
            _id: 0,
            ids: { $setUnion: [{ $map: { input: '$sgoHits', as: 'h', in: '$$h._id' } }, []] },
            keys: { $setUnion: [{ $map: { input: '$sgoHits', as: 'p', in: '$$p.key' } }, []] },
        },
    },
];

exports.eventSgoFacet = eventSgoFacet;