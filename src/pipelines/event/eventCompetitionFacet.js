const { keySeparator } = require('../constants');

const eventCompetitionFacet = [
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
    {
        $lookup: {
            from: 'competitions',
            let: { cid: '$stageInfo._externalCompetitionId', cs: '$stageInfo._externalCompetitionIdScope' },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [{ $eq: ['$_externalId', '$$cid'] }, { $eq: ['$_externalIdScope', '$$cs'] }],
                        },
                    },
                },
                { $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
            ],
            as: 'competitions',
        },
    },
    {
        $project: {
            ids: { $setUnion: [{ $map: { input: '$competitions', as: 'c', in: '$$c._id' } }, []] },
            keys: { $setUnion: [{ $map: { input: '$competitions', as: 'c', in: { $concat: ['$$c._externalId', keySeparator, '$$c._externalIdScope'] } } }, []] },
        },
    },
];

exports.eventCompetitionFacet = eventCompetitionFacet;