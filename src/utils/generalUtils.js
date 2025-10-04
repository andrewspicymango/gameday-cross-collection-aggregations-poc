const crypto = require('crypto');
const _ = require('lodash');

////////////////////////////////////////////////////////////////////////////////
/**
 * Convert an integer number of milliseconds into minutes and seconds.
 *
 * The function treats the millisecond value as an absolute duration (negative values are allowed
 * and will be converted using their absolute value). Milliseconds are converted to total seconds
 * using Math.floor; minutes are derived by integer division by 60 and seconds are the remainder
 * in the range 0–59. If the provided `ms` is not an integer, the function returns null.
 *
 * @function getMinutesAndSecondsFromMs
 * @param {number} ms - Integer number of milliseconds (can be negative). Non-integer values return null.
 * @param {boolean} [returnString=false] - If true, return a formatted string "<minutes> minutes, <seconds> seconds";
 *                                         otherwise return an object.
 * @returns {{minutes: number, seconds: number}|string|null} An object with numeric `minutes` and `seconds`,
 *                                                         a formatted string when requested, or null for invalid input.
 * @example
 * // returns { minutes: 1, seconds: 30 }
 * getMinutesAndSecondsFromMs(90000);
 * @example
 * // returns '1 minutes, 30 seconds'
 * getMinutesAndSecondsFromMs(90000, true);
 */
function normalize(value) {
	if (Array.isArray(value)) {
		return value.map(normalize); // preserve array order
	} else if (value && typeof value === 'object') {
		const sortedKeys = Object.keys(value).sort();
		const normalizedObj = {};
		for (const key of sortedKeys) {
			normalizedObj[key] = normalize(value[key]);
		}
		return normalizedObj;
	} else {
		return value;
	}
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Generate a deterministic version identifier for a value.
 *
 * The function normalizes the provided value using `normalize(obj)`, serializes
 * the normalized value to JSON, computes a SHA-256 hash of that JSON, and
 * returns both the hex-encoded hash and the parsed normalized object.
 *
 * @param {*} obj - The value to normalize and version (typically an object,
 *                  but any JSON-serializable value is accepted).
 * @returns {{version: string, obj: *}} An object containing:
 *   - version: Hex-encoded SHA-256 digest of the normalized JSON representation.
 *   - obj: The normalized value parsed back from the JSON string.
 *
 * @throws {Error} If normalization, JSON serialization, or the hashing process fails.
 *
 * @example
 * // returns { version: '...', obj: { a: 1, b: 2 } }
 * const result = generateVersionId({ b: 2, a: 1 });
 */
const generateVersionId = function (obj) {
	const json = JSON.stringify(normalize(obj));
	const hash = crypto.createHash('sha256').update(json).digest('hex');
	return { version: hash, obj: JSON.parse(json) };
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Validates a date string in the format YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS and converts it to ISO8601 format.
 *
 * This function checks if the provided date string is valid and converts it to an ISO8601 formatted string.
 * If the date string is in the format YYYY-MM-DD, it allows setting specific hours, minutes, and seconds.
 * The function can also return only the date portion if the `dateOnly` parameter is set to true.
 *
 * @param {string} dateString - The date string to validate and convert.
 * @param {number} [hh=0] - The hours to set (0-23) if the date string is in YYYY-MM-DD format.
 * @param {number} [mm=0] - The minutes to set (0-59) if the date string is in YYYY-MM-DD format.
 * @param {number} [ss=0] - The seconds to set (0-59) if the date string is in YYYY-MM-DD format.
 * @param {boolean} [dateOnly=false] - Whether to return only the date portion (YYYY-MM-DD).
 * @returns {string|null} The ISO8601 formatted date string if valid, or null if the date string is invalid.
 */
const validateAndConvertDate = function (dateString, hh = 0, mm = 0, ss = 0, dateOnly = false) {
	if (dateString === undefined) return null;
	if ((dateString == null || dateString === '') && !_.isDate(dateString)) return null;
	if (_.isDate(dateString)) {
		return dateOnly ? dateString.toISOString().split('T')[0] : dateString.toISOString();
	}
	const regexDateOnly = /^\d{4}-\d{2}-\d{2}$/;
	if (regexDateOnly.test(dateString)) {
		const date = new Date(dateString);
		if (!isNaN(date.getTime())) {
			if (hh >= 0 && hh <= 23 && mm >= 0 && mm <= 59 && ss >= 0 && ss <= 59) {
				date.setUTCHours(hh, mm, ss);
			}
			return dateOnly ? date.toISOString().split('T')[0] : date.toISOString();
		}
	} else {
		const date = new Date(dateString);
		if (!isNaN(date.getTime())) {
			return dateOnly ? date.toISOString().split('T')[0] : date.toISOString();
		}
	}

	return null;
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Normalize a port into a number, string, or false.
 *
 * Attempts to parse the provided value as a base-10 integer port. If parsing
 * yields a non-NaN number and the number is >= 0, the numeric port is returned.
 * If parsing yields NaN, the original value is returned (useful for named pipes).
 * If the parsed number is negative, false is returned to indicate an invalid port.
 *
 * @param {string|number} val - The port value to normalize (e.g. "3000", 3000, or a named pipe).
 * @returns {number|string|boolean} The normalized port: a non-negative number for valid ports,
 *                                  the original value for non-numeric inputs (named pipes),
 *                                  or false for invalid numeric ports (negative values).
 *
 * @example
 * // returns 3000
 * normalizePort("3000");
 *
 * @example
 * // returns "pipeName"
 * normalizePort("pipeName");
 *
 * @example
 * // returns false
 * normalizePort(-1);
 */
const normalizePort = function (val) {
	var port = parseInt(val, 10);
	if (isNaN(port)) {
		return val;
	}
	if (port >= 0) {
		return port;
	}
	return false;
};

////////////////////////////////////////////////////////////////////////////////
module.exports = {
	generateVersionId,
	validateAndConvertDate,
	normalize,
	normalizePort,
};
