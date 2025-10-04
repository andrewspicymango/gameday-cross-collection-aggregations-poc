require('dotenv').config({ quiet: true });

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
	},
	aws: {
		region: process.env.AWS_REGION || 'eu-west-1',
	},
	express: {
		port: process.env.EXPRESS_PORT || 8080,
	},
	serviceName: process.env.SERVICE_NAME || 'MTPage Service',
};
