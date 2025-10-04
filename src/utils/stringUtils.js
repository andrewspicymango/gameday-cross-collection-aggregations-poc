const _ = require('lodash');

////////////////////////////////////////////////////////////////////////////////
/**
 * Converts all non-breaking space characters in a string to standard space characters.
 *
 * @param {string} input - The input string to process.
 * @returns {string} - The processed string with non-breaking spaces replaced by standard spaces.
 */
const replaceNonBreakingSpaces = function (input) {
	if (typeof input !== 'string') {
		throw new TypeError('Input must be a string');
	}
	return input.replace(/\u00A0/g, ' ');
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Converts a camelCase string to snake_case.
 *
 * @param {string} str - The camelCase string to convert.
 * @returns {string} The converted snake_case string.
 */
const camelToSnakeCase = function (str) {
	return str
		.toString()
		.replace(/([a-z0-9])([A-Z])/g, '$1_$2') // Insert underscore between lowercase/number and uppercase
		.replace(/([A-Z])([A-Z][a-z])/g, '$1_$2') // Insert underscore between uppercase sequences followed by lowercase
		.toLowerCase();
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Capitalizes the first letter of a word.
 *
 * @param {string} word - The word to capitalize.
 * @returns {string|undefined} The capitalized word, or undefined if input is falsy.
 */
const capitalize = function (word) {
	if (word) return word.charAt(0).toUpperCase() + word.slice(1);
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Splits a camelCase string into different case formats.
 *
 * @param {string} str - The camelCase string to split and convert.
 * @returns {Object} An object containing titleCase, sentenceCase, and snakeCase versions.
 * @returns {string|null} returns.titleCase - Each word capitalized and space-separated.
 * @returns {string|null} returns.sentenceCase - Only first word capitalized.
 * @returns {string|null} returns.snakeCase - Underscore-separated lowercase.
 */
const splitCamelCase = function (str) {
	if (!_.isString(str) || str.length === 0) {
		return { titleCase: null, sentenceCase: null, snakeCase: null };
	}
	//////////////////////////////////////////////////////////////////////////////
	// Split on transitions from lower to upper or digit to upper
	const words = str.match(/([A-Z][a-z0-9]*)/g) || [];
	//////////////////////////////////////////////////////////////////////////////
	// Capitalize every word
	const titleCase = words.length > 0 ? words.map(capitalize).join(' ') : str;
	//////////////////////////////////////////////////////////////////////////////
	// Only capitalize the first word
	const sentenceCase = words.length > 0 ? [capitalize(words[0]), ...words.slice(1).map((w) => w.toLowerCase())].join(' ') : str;
	//////////////////////////////////////////////////////////////////////////////
	// Snake case
	const snakeCase = camelToSnakeCase(str);
	//////////////////////////////////////////////////////////////////////////////
	return { titleCase, sentenceCase, snakeCase };
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Checks if a value is a non-empty string.
 *
 * @param {*} value - The value to check.
 * @returns {boolean} True if the value is a string with non-whitespace content, false otherwise.
 */
const isNonEmptyString = function (value) {
	if (value === undefined) return false;
	return typeof value === 'string' && value.trim() !== '';
};

////////////////////////////////////////////////////////////////////////////////
module.exports = {
	replaceNonBreakingSpaces,
	camelToSnakeCase,
	splitCamelCase,
	isNonEmptyString,
};
