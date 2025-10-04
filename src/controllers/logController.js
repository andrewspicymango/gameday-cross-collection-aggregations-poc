const { send200, send500 } = require('../utils/httpResponseUtils');
const { setLogLevel, info, debug } = require('../log.js');
const uuid = require('uuid');
const config = require('../config.js');

////////////////////////////////////////////////////////////////////////////////
// Set debug log level endpoint
async function _setDebugLogLevel(req, res, next) {
	const id = uuid.v4();
	debug(`${req.method} ${req.url}${req.hostname != undefined ? ' [called from ' + req.hostname + ']' : ''}`, id);
	try {
		setLogLevel('debug');
		info(`Logging set to DEBUG`, id);
		send200(res, { status: 200, service: config?.serviceName, message: `Log level set to debug` }, config);
	} catch (err) {
		send500(res, err.message);
	}
}

////////////////////////////////////////////////////////////////////////////////
// Set info log level endpoint
async function _setInfoLogLevel(req, res, next) {
	const id = uuid.v4();
	debug(`${req.method} ${req.url}${req.hostname != undefined ? ' [called from ' + req.hostname + ']' : ''}`, id);
	try {
		setLogLevel('info');
		info(`Logging set to INFO`, id);
		send200(res, { status: 200, service: config?.serviceName, message: `Log level set to info` }, config);
	} catch (err) {
		send500(res, err.message);
	}
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { _setDebugLogLevel, _setInfoLogLevel };
