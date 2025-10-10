const _ = require('lodash');

////////////////////////////////////////////////////////////////////////////////
/**
 * Send a JSON-formatted error response using an Express-like response object.
 *
 * Behaviour:
 * - If `responseStringOrObject` is an object (per _.isObject) it attempts to serialize
 *   that object with `JSON.stringify(obj, null, 3)` and send it with a trailing newline.
 *   If serialization throws, a fallback object `{ status: code, error: 'Could not serialize response' }`
 *   is sent instead.
 * - If `responseStringOrObject` is not an object, it is coerced to a string (unless already a string)
 *   and wrapped into an object `{ status: code, error: <string> }` which is then serialized and sent.
 * - In all cases the response's Content-Type is set via `res.type('json')`, the HTTP status is set
 *   to `code` with `res.status(code)`, and the body is sent with `res.send(...)`.
 *
 * Notes:
 * - Expects `res` to implement an Express-like interface: `.type()`, `.status()`, and `.send()`.
 * - Uses lodash helpers `_.isObject` and `_.isString` to distinguish input types.
 * - JSON is pretty-printed with 3 spaces indentation and ends with a newline.
 *
 * @function sendError
 * @param {number} code - HTTP status code to send (e.g., 400, 500).
 * @param {Object} res - Express-like response object with `.type()`, `.status()`, and `.send()` methods.
 * @param {Object|string|any} responseStringOrObject - The error payload. If an object, it will be serialized
 *                                                     directly; otherwise it will be coerced to a string and
 *                                                     placed in an `{ status, error }` envelope.
 * @returns {void} Does not return a value; sends the response to the client.
 */
const sendError = function (code, res, responseStringOrObject) {
	if (_.isObject(responseStringOrObject)) {
		try {
			res
				.type('json')
				.status(code)
				.send(JSON.stringify(responseStringOrObject, null, 3) + '\n');
		} catch (err) {
			res
				.type('json')
				.status(code)
				.send(JSON.stringify({ status: code, error: `Could not serialize response` }, null, 3) + '\n');
		}
	} else {
		if (!_.isString(responseStringOrObject)) {
			responseStringOrObject = `${responseStringOrObject}`;
		}
		res
			.type('json')
			.status(code)
			.send(JSON.stringify({ status: code, error: responseStringOrObject }, null, 3) + '\n');
	}
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Send a 200 OK JSON response using the provided Express-like response object.
 *
 * If `responseStringOrObject` is an object (detected via `_.isObject`), it is
 * serialized and sent as-is (pretty-printed with 3-space indentation and a
 * trailing newline). If it is not an object, it will be wrapped into
 * { status: 200, message: "<value>" } and sent as JSON.
 *
 * If `config?.server?.accessControlAllowOrigin` is provided (non-null/undefined),
 * the function will append an `Access-Control-Allow-Origin` header with that value.
 *
 * The function:
 * - sets the response content type to JSON via `res.type('json')`,
 * - sets HTTP status to 200,
 * - optionally appends the Access-Control-Allow-Origin header,
 * - and sends the resulting JSON string.
 *
 * Note: This function depends on lodash's `_.isObject` and an Express-like
 * response API that supports `type`, `status`, `append`, and `send`.
 *
 * @function send200
 * @param {Object} res - Express response object (or compatible). Must implement `.type()`, `.status()`, `.append()`, and `.send()`.
 * @param {string|Object} responseStringOrObject - Response payload. If an object, it will be sent as JSON. Otherwise it will be converted to a `{ status: 200, message: string }` object before sending.
 * @param {Object} [config] - Optional configuration.
 * @param {Object} [config.server] - Server configuration container.
 * @param {string} [config.server.accessControlAllowOrigin] - If provided, value to set for the `Access-Control-Allow-Origin` header.
 * @returns {void} This function sends the response and does not return a value.
 *
 * @example
 * // send a simple message
 * send200(res, "ok", { server: { accessControlAllowOrigin: "*" } });
 *
 * @example
 * // send an object payload
 * send200(res, { id: 1, name: "Alice" });
 */
const send200 = function (res, responseStringOrObject, config, type = 'json') {
	const body = _.isObject(responseStringOrObject)
		? JSON.stringify(responseStringOrObject, null, 3) + '\n'
		: JSON.stringify({ status: 200, message: `${responseStringOrObject}` }, null, 3);
	if (config?.server?.accessControlAllowOrigin != null) {
		res.type(type).status(200).append('Access-Control-Allow-Origin', config.server.accessControlAllowOrigin).send(body);
	} else {
		res.type(type).status(200).send(body);
	}
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Convenience wrapper to send an HTTP 400 (Bad Request) response.
 * Delegates to sendError with a 400 status code.
 *
 * @param {Object} res - Response object (e.g., Express `res`) used to send the HTTP response.
 * @param {string|Object} [responseStringOrObject='Bad Request'] - Message string or response object to send as the body.
 * @returns {*} The value returned by sendError(400, res, responseStringOrObject).
 */
const send400 = function (res, responseStringOrObject = 'Bad Request') {
	return sendError(400, res, responseStringOrObject);
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Send a 401 Unauthorized HTTP response using the shared sendError helper.
 *
 * @param {import("express").Response|import("http").ServerResponse} res - The HTTP response object to send the status on.
 * @param {string|Object} [responseStringOrObject='Unauthorized'] - A message string or an object to use as the response body/payload.
 * @returns {*} The value returned by sendError (if any).
 * @see sendError
 */
const send401 = function (res, responseStringOrObject = 'Unauthorized') {
	return sendError(401, res, responseStringOrObject);
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Send a 404 Not Found response.
 *
 * Delegates to `sendError` with HTTP status code 404. The `responseStringOrObject`
 * parameter may be a string or an object to use as the response body; if omitted,
 * it defaults to `'No resource here'`.
 *
 * @param {object} res - HTTP response object (e.g., Express `res`) used to send the response.
 * @param {string|object} [responseStringOrObject='No resource here'] - Message or payload to include in the response body.
 * @returns {void}
 */
const send404 = function (res, responseStringOrObject = 'No resource here') {
	return sendError(404, res, responseStringOrObject);
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Send an HTTP 500 (Internal Server Error) response by delegating to sendError.
 *
 * @param {import('http').ServerResponse|import('express').Response} res - The response object to send the error on.
 * @param {string|object} [responseStringOrObject='Internal Server Error'] - Message string or object to include in the response body.
 * @returns {*} The value returned by sendError (may be void or whatever sendError returns).
 * @see sendError
 */
const send500 = function (res, responseStringOrObject = 'Internal Server Error') {
	return sendError(500, res, responseStringOrObject);
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Send an HTTP 503 (Service Busy) response by delegating to sendError.
 *
 * @param {import('http').ServerResponse|import('express').Response} res - The response object to send the error on.
 * @param {string|object} [responseStringOrObject='Service Busy'] - Message string or object to include in the response body.
 * @returns {*} The value returned by sendError (may be void or whatever sendError returns).
 * @see sendError
 */
const send503 = function (res, responseStringOrObject = 'Service Busy') {
	return sendError(503, res, responseStringOrObject);
};

////////////////////////////////////////////////////////////////////////////////
module.exports = { send200, send400, send401, send404, send500, send503, sendError };
