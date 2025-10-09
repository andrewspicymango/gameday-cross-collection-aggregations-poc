const { keySeparator } = require('../constants');

const eventStageFacet = [
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
                { $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
            ],
            as: 'stages',
        },
    },
    {
        $project: {
            ids: { $setUnion: [{ $map: { input: '$stages', as: 's', in: '$$s._id' } }, []] },
            keys: { $setUnion: [{ $map: { input: '$stages', as: 's', in: { $concat: ['$$s._externalId', keySeparator, '$$s._externalIdScope'] } } }, []] },
        },
    },
];

exports.eventStageFacet = eventStageFacet;