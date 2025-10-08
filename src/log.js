const _ = require('lodash');
const path = require('path');
const fs = require('fs');
const config = require('./config.js');
const logPath = config?.log?.logFilePath || path.join(__dirname, '../../scratch/logs');
const logLines = [];

////////////////////////////////////////////////////////////////////////////////
// Defined log levels
const loglevel = {
	DEBUG: { v: 0, s: `DEBUG` },
	INFO: { v: 10, s: `INFO` },
	WARNING: { v: 20, s: `WARNING` },
	ERROR: { v: 30, s: `ERROR` },
	setLevel: function (loglevel) {
		lv = loglevel.v;
	},
};

let lv = loglevel.DEBUG;
let levelPadLength = 0;
let scopePadLength = 0;
let contextPadLength = 0;
const PID = process.pid;

////////////////////////////////////////////////////////////////////////////////
function warn(msg, scope = null, context = null, useStackLevel = 4) {
	log(msg, loglevel.WARNING, scope, context, useStackLevel);
}

////////////////////////////////////////////////////////////////////////////////
function info(msg, scope = null, context = null, useStackLevel = 4) {
	log(msg, loglevel.INFO, scope, context, useStackLevel);
}

////////////////////////////////////////////////////////////////////////////////
function debug(msg, scope = null, context = null, useStackLevel = 4) {
	log(msg, loglevel.DEBUG, scope, context, useStackLevel);
}

////////////////////////////////////////////////////////////////////////////////
function error(msg, scope = null, context = null, useStackLevel = 4) {
	log(msg, loglevel.ERROR, scope, context, useStackLevel);
}

////////////////////////////////////////////////////////////////////////////////
function getCallerFunctionName(level = 4) {
	const error = new Error();
	const stack = error.stack.split('\n');
	const callerLine = stack[level >= stack.length ? stack.length - 1 : level]; // Adjust the index based on your environment
	const functionNameMatch = callerLine?.match(/at (\w+)/);
	const levelReport = functionNameMatch ? functionNameMatch[1] : null;
	if (levelReport && levelReport.length > contextPadLength && contextPadLength > 0) contextPadLength = levelReport.length + 1;
	return levelReport;
}

////////////////////////////////////////////////////////////////////////////////
function getCallerFileName(level = 4) {
	const error = new Error();
	const stack = error.stack.split('\n');
	const callerLine = stack[level >= stack.length ? stack.length - 1 : level]; // Adjust the index based on your environment
	const fileNameMatch = callerLine?.match(/\((.*):(\d+):\d+\)/);
	const levelReport = fileNameMatch ? `${path.basename(fileNameMatch[1])}:${fileNameMatch[2]}` : null;
	if (levelReport && levelReport.length > scopePadLength && scopePadLength > 0) scopePadLength = levelReport.length + 1;
	return levelReport;
}

////////////////////////////////////////////////////////////////////////////////
function log(msg, level = loglevel.INFO, scope = null, context = null, useStackLevel = 4) {
	if (msg == null) msg = `n/a`;
	if (_.isObject(msg)) msg = JSON.stringify(msg, null, 3);
	if (scope == null) scope = getCallerFileName(useStackLevel);
	if (context == null) context = getCallerFunctionName(useStackLevel);
	const levelStr = level.s.padEnd(levelPadLength, ' ');
	const scopeStr = _.isString(scope) ? scope.padEnd(scopePadLength, ' ') : ``;
	const contextStr = _.isString(context) ? context.padEnd(contextPadLength, ' ') : ``;
	const lines = msg.split(`\n`);
	for (const line of lines) {
		if (level.v >= lv.v) {
			const lineStr = `${PID}|${new Date().toISOString()}|${levelStr}|${scopeStr}|${contextStr}|${line}`;
			const lineObj = {
				pid: PID,
				time: new Date().toISOString(),
				level: levelStr,
				scope: scopeStr,
				context: contextStr,
				message: line,
			};
			console.log(config?.log?.useJson === true ? JSON.stringify(lineObj) : lineStr);
			logLines.unshift(lineObj);
			if (logLines.length > 1000) {
				logLines.splice(0, logLines.length - 1000);
			}
			if (config?.log?.writeToFile === true) {
				const logFilePath = path.join(logPath, `log.psv`);
				if (!fs.existsSync(logPath)) {
					fs.mkdirSync(logPath, { recursive: true });
					debug(`Creating ${logPath}`);
				}
				fs.appendFileSync(logFilePath, `${lineStr}\n`, { encoding: 'utf8' });
			}
		}
	}
}

/////////////////////////////////////////////////////////////////////////////////
function getLastLogLines() {
	return logLines;
}

////////////////////////////////////////////////////////////////////////////////
function logAndThrowError(msg, scope = 'n/a', context = 'n/a') {
	log(msg, loglevel.ERROR, scope, context);
	throw new Error(msg);
}

////////////////////////////////////////////////////////////////////////////////
function setLogLevel(logLevel) {
	if (logLevel == 'info') lv = loglevel.INFO;
	else if (logLevel == 'warn') lv = loglevel.WARNING;
	else if (logLevel == 'error') lv = loglevel.ERROR;
	else lv = loglevel.DEBUG;
}

////////////////////////////////////////////////////////////////////////////////
function getLogLevel() {
	if (lv == loglevel.INFO) return 'info';
	else if (lv == loglevel.WARNING) return 'warn';
	else if (lv == loglevel.ERROR) return 'error';
	else return 'debug';
}

////////////////////////////////////////////////////////////////////////////////
function setLevelPadLength(length) {
	levelPadLength = length;
}

////////////////////////////////////////////////////////////////////////////////
function setScopePadLength(length) {
	scopePadLength = length;
}

////////////////////////////////////////////////////////////////////////////////
function setContextPadLength(length) {
	contextPadLength = length;
}

// ////////////////////////////////////////////////////////////////////////////////
/**
 * Helper function to log progress.
 *
 * @param {number} currentIndex - The current index in the iteration.
 * @param {number} totalSize - The total size of the collection being processed.
 * @param {number} previousThresholdNumber - The previous threshold number percentage logged.
 * @param {string} message - The message to log with the progress.
 * @param {number} [numDecimalPlaces=0] - The number of decimal places to include in the progress report.
 * @returns {number} The current threshold number percentage.
 */
function logProgress(currentIndex, totalSize, previousThresholdNumber, message, numDecimalPlaces = 0) {
	const factor = Math.pow(10, numDecimalPlaces);
	const pc = ((currentIndex / totalSize) * 100).toFixed(numDecimalPlaces);
	const currentThresholdNumber = Math.floor(pc * factor) / factor;
	if (currentThresholdNumber > previousThresholdNumber) {
		const totalSizeStr = totalSize.toString();
		const idxStr = currentIndex.toString().padStart(totalSizeStr.length, ' ');
		info(`${message} - progress: ${currentThresholdNumber.toFixed(numDecimalPlaces)}% [${idxStr}/${totalSizeStr}]`);
		return currentThresholdNumber;
	}
	return previousThresholdNumber;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = {
	warn,
	info,
	debug,
	error,
	setLogLevel,
	getLogLevel,
	logAndThrowError,
	setLevelPadLength,
	setScopePadLength,
	setcontextPadLength: setContextPadLength,
	logProgress,
	getCallerFunctionName,
	getCallerFileName,
	getLastLogLines,
};
