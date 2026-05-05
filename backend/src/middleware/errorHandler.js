const logger = require('../utils/logger');

function errorHandler(err, req, res, next) {
  logger.error('Unhandled error', {
    message: err.message,
    stack: err.stack,
    path: req.path,
    method: req.method,
  });

  if (err.code === 'LIMIT_FILE_SIZE') {
    return res.status(413).json({ message: 'File too large. Maximum file size is 5GB.' });
  }

  // Handle upstream HTTP errors forwarded from axios proxy routes.
  // err.response is set by axios when the upstream service returns a non-2xx status.
  // The response body may be an ArrayBuffer (when responseType:'arraybuffer' was used),
  // so we decode it and extract the human-readable detail/message field.
  if (err.response) {
    const upstreamStatus = err.response.status;
    let upstreamMessage = '';
    const data = err.response.data;
    try {
      const text = Buffer.isBuffer(data)
        ? data.toString('utf8')
        : data instanceof ArrayBuffer
          ? Buffer.from(data).toString('utf8')
          : typeof data === 'string'
            ? data
            : JSON.stringify(data);
      const parsed = JSON.parse(text);
      upstreamMessage = parsed.detail || parsed.message || parsed.error || '';
    } catch (_) {
      // Ignore JSON parse failures — fall back to the generic Axios message below.
    }
    return res.status(upstreamStatus).json({ message: upstreamMessage || err.message });
  }

  const status = err.status || 500;
  const message = status < 500 ? err.message : 'Internal server error';
  res.status(status).json({ message });
}

module.exports = errorHandler;
