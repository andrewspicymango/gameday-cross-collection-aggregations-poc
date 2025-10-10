const { keySeparator } = require('./constants');
const _ = require('lodash');

////////////////////////////////////////////////////////////////////////////////
function splitKey(key) {
	if (!_.isString(key)) return { id: null, scope: null };
	return {
		id: key.split(keySeparator)[0] || null,
		scope: key.split(keySeparator)[1] || null,
	};
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { splitKey };
