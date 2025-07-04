const requestCounts = new Map()
const WINDOW_SIZE = 60 * 1000 // 1 minuto
const MAX_REQUESTS = 100 // 100 requests por minuto

export function rateLimiter(req, res, next) {
  const clientIP = req.ip || req.connection.remoteAddress
  const now = Date.now()
  
  // Limpiar requests antiguos
  if (requestCounts.has(clientIP)) {
    const requests = requestCounts.get(clientIP)
    const recentRequests = requests.filter(time => now - time < WINDOW_SIZE)
    requestCounts.set(clientIP, recentRequests)
  }
  
  // Obtener requests actuales
  const currentRequests = requestCounts.get(clientIP) || []
  
  // Verificar límite
  if (currentRequests.length >= MAX_REQUESTS) {
    return res.status(429).json({
      error: 'Too many requests',
      message: 'Rate limit exceeded. Try again later.',
      retryAfter: Math.ceil(WINDOW_SIZE / 1000)
    })
  }
  
  // Agregar request actual
  currentRequests.push(now)
  requestCounts.set(clientIP, currentRequests)
  
  // Headers informativos
  res.set({
    'X-RateLimit-Limit': MAX_REQUESTS,
    'X-RateLimit-Remaining': MAX_REQUESTS - currentRequests.length,
    'X-RateLimit-Reset': new Date(now + WINDOW_SIZE).toISOString()
  })
  
  next()
}