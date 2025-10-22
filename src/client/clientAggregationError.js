////////////////////////////////////////////////////////////////////////////////
class ClientAggregationError extends Error {
	constructor(message, code = 'CLIENT_AGGREGATION_ERROR', details = {}) {
		super(message);
		this.name = 'ClientAggregationError';
		this.code = code;
		this.isClientError = true; // convenient flag for error handlers
		this.details = details;
		Error.captureStackTrace?.(this, this.constructor);
	}
}

////////////////////////////////////////////////////////////////////////////////
class ServerAggregationError extends Error {
	constructor(message, code = 'SERVER_AGGREGATION_ERROR') {
		super(message);
		this.name = 'ServerAggregationError';
		this.code = code;
		this.isClientError = false; // convenient flag for error handlers
		Error.captureStackTrace?.(this, this.constructor);
	}
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { ClientAggregationError, ServerAggregationError };
