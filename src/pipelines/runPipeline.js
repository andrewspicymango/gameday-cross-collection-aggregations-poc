const { debug, warn } = require('../log');

////////////////////////////////////////////////////////////////////////////////
async function runPipeline(mongo, targetCollection, pipeline, requestId) {
	try {
		const startTime = process.hrtime.bigint();
		debug(`Started Aggregation Pipeline for ${targetCollection} aggregation view`, requestId);
		const result = await mongo.db.collection(targetCollection).aggregate(pipeline).toArray();
		const endTime = process.hrtime.bigint();
		const durationMs = Number(endTime - startTime) / 1000000; // Convert nanoseconds to milliseconds
		debug(`Finished Aggregation Pipeline for ${targetCollection} aggregation view in ${durationMs.toFixed(2)}ms`, requestId);
		return result;
	} catch (error) {
		warn(`Error running aggregation pipeline for ${targetCollection}: ${error.message}`, requestId);
		throw error;
	}
}

////////////////////////////////////////////////////////////////////////////////
module.exports = runPipeline;
