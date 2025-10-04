const fs = require('fs');
const path = require('path');
const _ = require('lodash');
const { error, debug, logAndThrowError } = require('../log.js');

////////////////////////////////////////////////////////////////////////////////
/**
 * Read a file synchronously using UTF-8 encoding and return its contents.
 *
 * This function performs a blocking filesystem read (fs.readFileSync) for the
 * provided path, emits a debug message indicating the path that was read, and
 * returns the file contents as a string. If an error occurs during the read,
 * the error message is delegated to logAndThrowError which logs and re-throws
 * the error.
 *
 * @param {string} path - The filesystem path to the file to read.
 * @returns {string} The file contents decoded as a UTF-8 string.
 * @throws {Error} When reading the file fails; the error is handled and re-thrown via logAndThrowError.
 */
const readFile = function (path) {
	try {
		const c = fs.readFileSync(path, 'utf8');
		debug(`Read data from ${path}`);
		return c;
	} catch (e) {
		logAndThrowError(e.message);
	}
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Read a file synchronously and return its contents encoded as a Base64 string.
 *
 * Uses fs.readFileSync with the encoding set to "base64". If reading the file fails,
 * the underlying error message is passed to logAndThrowError, which logs the error
 * and rethrows it.
 *
 * @param {string} path - The filesystem path (absolute or relative) to the file to read.
 * @returns {string} The file contents encoded as a Base64 string.
 * @throws {Error} When reading the file fails; the error is logged and rethrown by logAndThrowError.
 */
const readFileB64 = function (path) {
	try {
		const c = fs.readFileSync(path, { encoding: 'base64' });
		debug(`Read data in base 64 from ${path}`);
		return c;
	} catch (e) {
		logAndThrowError(e.message);
	}
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Determine whether the given filesystem path is a directory.
 *
 * Uses fs.lstatSync(path).isDirectory() to check the file system entry.
 * If the path exists but is not a directory, this function calls error(...)
 * with a descriptive message and returns false.
 *
 * Note: fs.lstatSync may throw (for example, if the path does not exist or
 * if there are permission problems). Such errors are not caught by this
 * function and will propagate to the caller.
 *
 * @param {string} path - The filesystem path to check.
 * @returns {boolean} True if the path exists and is a directory; false if it
 *                    exists but is not a directory.
 * @throws {Error} If fs.lstatSync throws (e.g., ENOENT for non-existent path or permission errors).
 */
const isDirectory = function (path) {
	if (!fs.lstatSync(path).isDirectory()) {
		error(`Looked for directory ${path}, but it does not exist`);
		return false;
	}
	return true;
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Ensure a directory exists at the specified filesystem path.
 *
 * If the directory does not already exist, it is created synchronously using
 * fs.mkdirSync with the { recursive: true } option so intermediate directories
 * will be created as needed. A debug message is emitted indicating whether the
 * directory was created or skipped because it already exists.
 *
 * This function performs synchronous filesystem operations and may throw an
 * error if the underlying operations fail (for example, due to permissions).
 *
 * @param {string} file - The filesystem path of the file to write.
 * @param {string|Buffer|Uint8Array} data - The data to write to the file.
 * @param {object} options - Optional write options.
 * @throws {Error} If a filesystem operation fails.
 * @returns {void}
 */
const writeFile = function (file, data, options = null) {
	try {
		if (options == null) {
			fs.writeFileSync(file, data);
		} else {
			fs.writeFileSync(file, data, options);
		}
		debug(`Wrote data to ${file}`);
	} catch (e) {
		logAndThrowError(e.message);
	}
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Ensure a directory exists at the given filesystem path.
 *
 * Synchronously checks whether the path exists and, if it does not, creates
 * the directory and any necessary parent directories using fs.mkdirSync with
 * the { recursive: true } option. Logs creation or skipping via debug.
 *
 * Note: This function is synchronous and may block the event loop. For
 * performance-sensitive or concurrent applications, prefer the asynchronous
 * fs APIs.
 *
 * @param {string} path - The filesystem path of the directory to ensure exists.
 * @returns {void}
 * @throws {Error} If the directory cannot be created (for example due to
 *                 permission issues or invalid path), the underlying fs
 *                 error will be thrown.
 * @example
 * // Ensure /tmp/my/nested/dir exists
 * createDir('/tmp/my/nested/dir');
 */
const createDir = function (path) {
	if (!fs.existsSync(path)) {
		fs.mkdirSync(path, { recursive: true });
		debug(`Creating ${path}`);
	} else {
		debug(`Skipping creating ${path} - already exists`);
	}
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Synchronously reads the entries of the specified directory and returns their names.
 *
 * @param {string} directoryPath - The path to the directory to read (absolute or relative).
 * @returns {string[]} An array of file and subdirectory names contained in the directory.
 * @throws {Error} If reading the directory fails. The error is logged and rethrown via logAndThrowError.
 */
const readFilesInDir = function (directoryPath) {
	try {
		const fileList = fs.readdirSync(directoryPath);
		return fileList;
	} catch (e) {
		logAndThrowError(e.message);
	}
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Ensures that a directory exists. If the directory does not exist, it creates
 * it.
 *
 * @param {string} directoryPath - The path of the directory to ensure.
 * @returns {string} The path of the directory.
 */
const ensureDirectoryExists = function (directoryPath) {
	if (!fs.existsSync(directoryPath)) {
		fs.mkdirSync(directoryPath, { recursive: true });
	}
	return directoryPath;
};

////////////////////////////////////////////////////////////////////////////////
/**
 * Synchronously writes data to a file, creating the path to the file if it does not exist.
 *
 * @param {string} filePath - The path of the file to write.
 * @param {string|Buffer|Uint8Array} data - The data to write to the file.
 * @throws {Error} If there is an error during the file writing process.
 */
const writeFileSyncWithDirs = function (filePath, data) {
	const directoryPath = path.dirname(filePath);
	ensureDirectoryExists(directoryPath);
	fs.writeFileSync(filePath, data);
};

////////////////////////////////////////////////////////////////////////////////
module.exports = {
	readFile,
	readFileB64,
	isDirectory,
	createDir,
	writeFile,
	readFilesInDir,
	writeFileSyncWithDirs,
	ensureDirectoryExists,
};
