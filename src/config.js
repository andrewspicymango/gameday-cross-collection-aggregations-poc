require('dotenv').config({ quiet: true });

// 'bblapi/2023:BBL',
// fifa/1jt5mxgn4q5r6mknmlqv5qjh0
module.exports = {
	log: {
		writeToFile: true,
		useJson: true,
		logFilePath: process.env.LOG_PATH || './scratch/logs',
	},
	mongo: {
		client: null,
		db: null,
		url: process.env.MONGOURL || null,
		dbName: process.env.MONGODB || 'gameday',
		matAggCollectionName: process.env.MAT_AGG_COLLECTION_NAME || 'materialisedAggregations',
		matAggIndexIdAndScope: process.env.MAT_AGG_INDEX_ID_AND_SCOPE || { resourceType: 1, externalKey: 1 },
		matAggIndexIdAndScopeName: process.env.MAT_AGG_INDEX_ID_AND_SCOPE_NAME || 'resourceType_1_externalKey_1', // Name of the index created on the matAggCollectionName
	},
	aws: {
		region: process.env.AWS_REGION || 'eu-west-1',
	},
	express: {
		port: process.env.EXPRESS_PORT || 8080,
	},
	serviceName: process.env.SERVICE_NAME || 'MTPage Service',
};
