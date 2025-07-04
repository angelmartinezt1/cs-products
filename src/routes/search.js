import { Router } from 'express'
import { rateLimiter } from '../middleware/rateLimiter.js'
import { validateSearchParams } from '../middleware/validation.js'
import { SearchService } from '../services/searchService.js'

const router = Router()

// Aplicar rate limiting a todas las rutas de búsqueda
router.use(rateLimiter)

/**
 * GET /api/search
 * Búsqueda principal de productos
 * 
 * Query params:
 * - q: string - término de búsqueda
 * - page: number - página (default: 1)
 * - limit: number - productos por página (default: 20, max: 100)
 * - sort: string - ordenamiento (relevance, price, rating, newest, name, reviews, discount)
 * - order: string - dirección (asc, desc)
 * - facets: boolean - incluir facetas (default: true)
 * - brand: string|array - filtro por marca(s)
 * - category_id: number - filtro por categoría
 * - category_lvl0: string - filtro por categoría nivel 0
 * - category_lvl1: string - filtro por categoría nivel 1
 * - store_id: number|array - filtro por tienda(s)
 * - min_price: number - precio mínimo
 * - max_price: number - precio máximo
 * - min_rating: number - rating mínimo
 * - free_shipping: boolean - solo envío gratis
 * - fulfillment_type: string - tipo de fulfillment
 * - digital: boolean - solo productos digitales
 * - has_discount: boolean - solo productos con descuento
 */
router.get('/', validateSearchParams, async (req, res, next) => {
  try {
    const startTime = Date.now()
    
    const searchParams = {
      query: req.query.q || '',
      page: parseInt(req.query.page) || 1,
      limit: Math.min(parseInt(req.query.limit) || 20, 100),
      filters: {
        brand: req.query.brand,
        category_id: req.query.category_id,
        min_price: req.query.min_price,
        max_price: req.query.max_price,
        min_rating: req.query.min_rating,
        free_shipping: req.query.free_shipping,
        fulfillment_type: req.query.fulfillment_type,
        sort: req.query.sort,
        order: req.query.order
      }
    }
    
    console.log('🔍 Searching with params:', searchParams)
    
    const results = await SearchService.simpleSearch(
      searchParams.query, 
      searchParams.page, 
      searchParams.limit,
      searchParams.filters
    )
    
    res.json({
      products: results.products,
      pagination: {
        page: results.page,
        limit: results.limit,
        total: results.total,
        totalPages: Math.ceil(results.total / results.limit)
      },
      query: searchParams.query,
      filters: searchParams.filters,
      meta: {
        executionTime: Date.now() - startTime,
        timestamp: new Date().toISOString()
      }
    })
    
  } catch (error) {
    console.error('Route error:', error)
    next(error)
  }
})

/**
 * GET /api/search/autocomplete
 * Autocompletado de búsquedas
 */
router.get('/autocomplete', async (req, res, next) => {
  try {
    const { q: query, limit = 10 } = req.query
    
    if (!query || query.length < 2) {
      return res.json({ suggestions: [] })
    }

    const suggestions = await SearchService.getAutocompleteSuggestions(
      query, 
      Math.min(parseInt(limit), 20)
    )

    res.json({ 
      suggestions,
      query,
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * GET /api/search/similar/:productId
 * Productos similares
 */
router.get('/similar/:productId', async (req, res, next) => {
  try {
    const { productId } = req.params
    const { limit = 12 } = req.query

    // Obtener producto base
    const baseProduct = await SearchService.getProductById(productId)
    if (!baseProduct) {
      return res.status(404).json({ error: 'Product not found' })
    }

    // Buscar productos similares por categoría y marca
    const searchParams = {
      query: '',
      page: 1,
      limit: parseInt(limit),
      sortBy: 'relevance',
      sortOrder: 'desc',
      facets: false,
      filters: {
        category_id: baseProduct.category_id,
        // Opcional: mismo fulfillment type
        // fulfillment_type: baseProduct.fulfillment_type
      }
    }

    // Excluir el producto actual
    const results = await SearchService.searchProducts(searchParams)
    const similarProducts = results.products.filter(p => p.id !== parseInt(productId))

    res.json({
      baseProduct: {
        id: baseProduct.id,
        name: baseProduct.name,
        category: baseProduct.category_name
      },
      similar: similarProducts.slice(0, limit),
      total: similarProducts.length
    })
  } catch (error) {
    next(error)
  }
})

/**
 * GET /api/search/trending
 * Productos trending/populares
 */
router.get('/trending', async (req, res, next) => {
  try {
    const { limit = 20, category_id } = req.query

    const searchParams = {
      query: '',
      page: 1,
      limit: parseInt(limit),
      sortBy: 'reviews', // Ordenar por cantidad de reviews como proxy de popularidad
      sortOrder: 'desc',
      facets: false,
      filters: {
        ...(category_id && { category_id: parseInt(category_id) })
      }
    }

    const results = await SearchService.searchProducts(searchParams)

    res.json({
      trending: results.products,
      category_id: category_id || null,
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * POST /api/search/batch
 * Búsqueda por lotes (múltiples queries)
 */
router.post('/batch', async (req, res, next) => {
  try {
    const { queries } = req.body
    
    if (!Array.isArray(queries) || queries.length === 0) {
      return res.status(400).json({ error: 'queries array is required' })
    }

    if (queries.length > 5) {
      return res.status(400).json({ error: 'Maximum 5 queries allowed per batch' })
    }

    // Ejecutar búsquedas en paralelo
    const results = await Promise.all(
      queries.map(async (queryParams, index) => {
        try {
          const searchParams = {
            query: queryParams.q || '',
            page: parseInt(queryParams.page) || 1,
            limit: Math.min(parseInt(queryParams.limit) || 10, 50),
            sortBy: queryParams.sort || 'relevance',
            sortOrder: queryParams.order || 'desc',
            facets: queryParams.facets !== 'false',
            filters: extractFilters(queryParams)
          }

          const result = await SearchService.searchProducts(searchParams)
          return { index, success: true, data: result }
        } catch (error) {
          return { index, success: false, error: error.message }
        }
      })
    )

    res.json({
      results,
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

// Helper function para extraer filtros de query params
function extractFilters(query) {
  const filters = {}
  
  // Filtros simples
  const simpleFilters = [
    'category_id', 'category_lvl0', 'category_lvl1', 'category_lvl2',
    'brand', 'store_id', 'fulfillment_type', 'min_price', 'max_price', 
    'min_rating', 'free_shipping', 'digital', 'has_discount'
  ]
  
  simpleFilters.forEach(filter => {
    if (query[filter] !== undefined) {
      // Convertir strings de arrays separados por coma
      if (typeof query[filter] === 'string' && query[filter].includes(',')) {
        filters[filter] = query[filter].split(',').map(v => v.trim())
      } else {
        filters[filter] = query[filter]
      }
    }
  })
  
  return filters
}

export { router as searchRoutes }
