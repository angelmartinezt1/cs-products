export function requestLogger(req, res, next) {
  const startTime = Date.now()
  const { method, url, query, ip } = req
  
  console.log(`[${new Date().toISOString()}] ${method} ${url}`, {
    ip,
    query: Object.keys(query).length ? query : undefined,
    userAgent: req.get('User-Agent')
  })
  
  // Log response time
  res.on('finish', () => {
    const duration = Date.now() - startTime
    console.log(`[${new Date().toISOString()}] ${method} ${url} - ${res.statusCode} (${duration}ms)`)
  })
  
  next()
}