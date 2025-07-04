const cacheStore = new Map()

export function cache(duration = 300) { // 5 minutos por defecto
  return (req, res, next) => {
    // Solo cachear GET requests
    if (req.method !== 'GET') {
      return next()
    }
    
    const key = req.originalUrl
    const cached = cacheStore.get(key)
    
    if (cached && Date.now() - cached.timestamp < duration * 1000) {
      res.set('X-Cache', 'HIT')
      return res.json(cached.data)
    }
    
    // Interceptar res.json para guardar en cache
    const originalJson = res.json
    res.json = function(data) {
      cacheStore.set(key, {
        data,
        timestamp: Date.now()
      })
      res.set('X-Cache', 'MISS')
      return originalJson.call(this, data)
    }
    
    next()
  }
}

// Limpiar cache periódicamente
setInterval(() => {
  const now = Date.now()
  const maxAge = 10 * 60 * 1000 // 10 minutos
  
  for (const [key, value] of cacheStore.entries()) {
    if (now - value.timestamp > maxAge) {
      cacheStore.delete(key)
    }
  }
}, 5 * 60 * 1000) // Cada 5 minutos