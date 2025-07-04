// src/middleware/facetsMiddleware.js
/**
 * Middleware para normalizar y validar parámetros de facetas
 */
export function normalizeFacetParams(req, res, next) {
  const { query } = req

  // Normalizar arrays de filtros
  const arrayFields = ['brand', 'store_id', 'category_id']
  arrayFields.forEach(field => {
    if (query[field] && typeof query[field] === 'string') {
      // Convertir string separado por comas a array
      query[field] = query[field].split(',').map(item => item.trim()).filter(Boolean)
    }
  })

  // Normalizar valores booleanos
  const booleanFields = ['free_shipping', 'digital', 'has_discount', 'big_ticket', 'super_express']
  booleanFields.forEach(field => {
    if (query[field] !== undefined) {
      query[field] = query[field] === 'true' || query[field] === '1'
    }
  })

  // Normalizar valores numéricos
  const numericFields = ['min_price', 'max_price', 'min_rating', 'limit', 'page']
  numericFields.forEach(field => {
    if (query[field] !== undefined && query[field] !== '') {
      const num = parseFloat(query[field])
      if (!isNaN(num)) {
        query[field] = num
      } else {
        delete query[field]
      }
    }
  })

  // Validar rangos de precio
  if (query.min_price && query.max_price && query.min_price > query.max_price) {
    return res.status(400).json({
      error: 'Invalid price range',
      message: 'min_price cannot be greater than max_price'
    })
  }

  // Validar rating
  if (query.min_rating && (query.min_rating < 0 || query.min_rating > 5)) {
    return res.status(400).json({
      error: 'Invalid rating',
      message: 'min_rating must be between 0 and 5'
    })
  }

  // Limpiar query de parámetros vacíos
  Object.keys(query).forEach(key => {
    if (query[key] === '' || query[key] === null || query[key] === undefined) {
      delete query[key]
    }
  })

  next()
}

/**
 * Middleware para agregar información de facetas a respuestas de búsqueda
 */
export function attachFacetsInfo(req, res, next) {
  const originalJson = res.json.bind(res)
  
  res.json = function(data) {
    // Solo agregar facetas si es una respuesta de búsqueda
    if (data && data.products && !data.facets && req.query.include_facets !== 'false') {
      // Agregar información sobre disponibilidad de facetas
      data.facetsAvailable = true
      data.facetsUrl = `${req.protocol}://${req.get('host')}/api/facets?${new URLSearchParams(req.query).toString()}`
    }
    
    return originalJson.call(this, data)
  }
  
  next()
}

