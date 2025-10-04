const uuid = require('uuid');
const { send200 } = require('../utils/httpResponseUtils');
const { debug } = require('../log.js');

////////////////////////////////////////////////////////////////////////////////
/**
 * Express handler for a simple healthcheck endpoint.
 *
 * Sends an HTTP 200 response confirming the server is running.
 *
 * @param {import('express').Request} req - The Express request object.
 * @param {import('express').Response} res - The Express response object.
 * @returns {void} Does not return a value; sends a response to the client.
 */
function healthcheckController(req, res) {
	const id = uuid.v4();
	debug(`${req.method} ${req.url}${req.hostname != undefined ? ' [called from ' + req.hostname + ']' : ''}`, id);
	send200(res, 'Healthcheck! Server is running.');
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { healthcheckController };
