// src/routes/facets.js - Versión mejorada
import { Router } from 'express'
import { executeQuery } from '../config/database.js'
import { cache } from '../middleware/cache.js'
import { FacetService } from '../services/facetService.js'

const router = Router()

/**
 * GET /api/facets
 * Obtener facetas dinámicas basadas en búsqueda y filtros actuales
 */
router.get('/', async (req, res, next) => {
  try {
    const {
      q: searchQuery = '',
      category_id: categoryId,
      use_cache = 'false',
      ...filters
    } = req.query

    // Si se solicita usar cache y no hay filtros específicos, usar facetas pre-calculadas
    if (use_cache === 'true' && !searchQuery && Object.keys(filters).length === 0) {
      const quickFacets = await FacetService.getQuickFacets(categoryId)
      return res.json({
        facets: quickFacets,
        type: 'cached',
        category_id: categoryId || null,
        timestamp: new Date().toISOString()
      })
    }

    // Obtener facetas dinámicas
    const facets = await FacetService.getFacets(searchQuery, filters, categoryId)
    
    res.json({
      ...facets,
      type: 'dynamic',
      category_id: categoryId || null,
      searchQuery: searchQuery || null,
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * GET /api/facets/categories/hierarchy
 * Jerarquía completa de categorías con conteos CORREGIDOS
 */
router.get('/categories/hierarchy', cache(1800), async (req, res, next) => {
  try {
    const { 
      category_id,
      max_depth = 3,
      min_products = 1 
    } = req.query

    let sql = `
      SELECT 
        p.category_id,
        p.category_name,
        p.category_lvl0,
        p.category_lvl1,
        p.category_lvl2,
        p.category_path,
        COUNT(*) as product_count,
        AVG(p.sales_price) as avg_price,
        MIN(p.sales_price) as min_price,
        MAX(p.sales_price) as max_price,
        AVG(p.review_rating) as avg_rating
      FROM products p
      WHERE p.status = 1 AND p.visible = 1 AND p.store_authorized = 1
        AND p.category_lvl0 IS NOT NULL
    `
    const params = []

    if (category_id) {
      sql += ` AND p.category_id = ?`
      params.push(category_id)
    }

    sql += `
      GROUP BY p.category_id, p.category_name, p.category_lvl0, p.category_lvl1, p.category_lvl2, p.category_path
      HAVING product_count >= ?
      ORDER BY p.category_lvl0, p.category_lvl1, p.category_lvl2, product_count DESC
    `
    params.push(parseInt(min_products))

    const results = await executeQuery(sql, params)

    // Construir jerarquía con conteos CORREGIDOS
    const hierarchy = {}
    
    results.forEach(row => {
      const lvl0 = row.category_lvl0
      const lvl1 = row.category_lvl1
      const lvl2 = row.category_lvl2
      const productCount = parseInt(row.product_count) // CONVERSIÓN A NÚMERO

      // Nivel 0
      if (!hierarchy[lvl0]) {
        hierarchy[lvl0] = {
          name: lvl0,
          level: 0,
          productCount: 0,                    // INICIALIZAR COMO NÚMERO
          priceRange: { min: Infinity, max: 0 },
          avgRating: 0,
          children: {}
        }
      }

      hierarchy[lvl0].productCount += productCount    // SUMA NUMÉRICA

      hierarchy[lvl0].priceRange.min = Math.min(hierarchy[lvl0].priceRange.min, row.min_price)
      hierarchy[lvl0].priceRange.max = Math.max(hierarchy[lvl0].priceRange.max, row.max_price)

      // Nivel 1
      if (lvl1 && lvl1 !== lvl0 && parseInt(max_depth) >= 2) {
        if (!hierarchy[lvl0].children[lvl1]) {
          hierarchy[lvl0].children[lvl1] = {
            name: lvl1,
            level: 1,
            productCount: 0,                  // INICIALIZAR COMO NÚMERO
            priceRange: { min: Infinity, max: 0 },
            children: {}
          }
        }

        hierarchy[lvl0].children[lvl1].productCount += productCount  // SUMA NUMÉRICA

        hierarchy[lvl0].children[lvl1].priceRange.min = Math.min(hierarchy[lvl0].children[lvl1].priceRange.min, row.min_price)
        hierarchy[lvl0].children[lvl1].priceRange.max = Math.max(hierarchy[lvl0].children[lvl1].priceRange.max, row.max_price)

        // Nivel 2
        if (lvl2 && lvl2 !== lvl1 && parseInt(max_depth) >= 3) {
          if (!hierarchy[lvl0].children[lvl1].children[lvl2]) {
            hierarchy[lvl0].children[lvl1].children[lvl2] = {
              categoryId: row.category_id,
              name: lvl2,
              level: 2,
              productCount: productCount,     // YA ES NÚMERO
              priceRange: {
                min: parseFloat(row.min_price),
                max: parseFloat(row.max_price),
                avg: parseFloat(row.avg_price)
              },
              avgRating: parseFloat(row.avg_rating) || 0
            }
          } else {
            // Si ya existe, sumar productos
            hierarchy[lvl0].children[lvl1].children[lvl2].productCount += productCount
          }
        }
      }
    })

    // Convertir a array y limpiar
    const categoryTree = Object.values(hierarchy).map(lvl0 => ({
      ...lvl0,
      priceRange: {
        min: lvl0.priceRange.min === Infinity ? 0 : lvl0.priceRange.min,
        max: lvl0.priceRange.max
      },
      children: Object.values(lvl0.children).map(lvl1 => ({
        ...lvl1,
        priceRange: {
          min: lvl1.priceRange.min === Infinity ? 0 : lvl1.priceRange.min,
          max: lvl1.priceRange.max
        },
        children: Object.values(lvl1.children)
          .sort((a, b) => b.productCount - a.productCount)
      }))
      .sort((a, b) => b.productCount - a.productCount)
    }))
    .sort((a, b) => b.productCount - a.productCount)

    res.json({
      hierarchy: categoryTree,
      totalCategories: results.length,
      maxDepth: parseInt(max_depth),
      minProducts: parseInt(min_products),
      explanation: {
        productCount: "Número total de productos disponibles en esta categoría y subcategorías",
        priceRange: "Rango de precios mínimo y máximo de productos en esta categoría",
        avgRating: "Calificación promedio de productos en esta categoría",
        levels: {
          0: "Categoría principal (ej: Electrónicos)",
          1: "Subcategoría (ej: Teléfonos y Accesorios)", 
          2: "Categoría específica (ej: Smartphones iPhone)"
        }
      },
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * GET /api/facets/stores/top
 * Tiendas principales con métricas detalladas
 */
router.get('/stores/top', cache(900), async (req, res, next) => {
  try {
    const { 
      limit = 15,
      category_id,
      sort_by = 'products' // products, rating, reviews
    } = req.query

    let orderClause
    switch (sort_by) {
      case 'rating':
        orderClause = 'avg_rating DESC, product_count DESC'
        break
      case 'reviews':
        orderClause = 'total_reviews DESC, avg_rating DESC'
        break
      default:
        orderClause = 'product_count DESC, avg_rating DESC'
    }

    let sql = `
      SELECT 
        p.store_id,
        p.store_name,
        p.store_logo,
        AVG(p.store_rating) as avg_rating,
        COUNT(*) as product_count,
        SUM(p.total_reviews) as total_reviews,
        AVG(p.sales_price) as avg_price,
        MIN(p.sales_price) as min_price,
        MAX(p.sales_price) as max_price,
        COUNT(CASE WHEN p.has_free_shipping = 1 THEN 1 END) as free_shipping_count,
        COUNT(CASE WHEN p.fulfillment_type = 'fulfillment' THEN 1 END) as fulfillment_count,
        COUNT(CASE WHEN p.percentage_discount > 0 THEN 1 END) as discounted_count,
        AVG(p.shipping_days) as avg_shipping_days,
        COUNT(DISTINCT p.brand) as brand_count,
        COUNT(DISTINCT p.category_id) as category_count
      FROM products p
      WHERE p.status = 1 AND p.visible = 1 AND p.store_authorized = 1
    `
    const params = []

    if (category_id) {
      sql += ` AND p.category_id = ?`
      params.push(category_id)
    }

    sql += `
      GROUP BY p.store_id, p.store_name, p.store_logo
      HAVING product_count >= 5
      ORDER BY ${orderClause}
      LIMIT ?
    `
    params.push(parseInt(limit))

    const results = await executeQuery(sql, params)

    res.json({
      topStores: results.map(row => ({
        storeId: row.store_id,
        storeName: row.store_name,
        storeLogo: row.store_logo,
        metrics: {
          rating: parseFloat(row.avg_rating) || 0,
          productCount: row.product_count,
          totalReviews: row.total_reviews || 0,
          brandCount: row.brand_count,
          categoryCount: row.category_count
        },
        pricing: {
          avgPrice: parseFloat(row.avg_price),
          minPrice: parseFloat(row.min_price),
          maxPrice: parseFloat(row.max_price)
        },
        services: {
          freeShippingPercentage: Math.round((row.free_shipping_count / row.product_count) * 100),
          fulfillmentPercentage: Math.round((row.fulfillment_count / row.product_count) * 100),
          discountPercentage: Math.round((row.discounted_count / row.product_count) * 100),
          avgShippingDays: Math.round(row.avg_shipping_days) || 0
        }
      })),
      sortedBy: sort_by,
      category_id: category_id || null,
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * GET /api/facets/search-suggestions
 * Sugerencias de filtros basadas en la búsqueda actual
 */
router.get('/search-suggestions', async (req, res, next) => {
  try {
    const { 
      q: searchQuery = '',
      category_id,
      limit = 10
    } = req.query

    if (!searchQuery || searchQuery.length < 2) {
      return res.json({ suggestions: [] })
    }

    // Buscar sugerencias en marcas
    const brandSql = `
      SELECT DISTINCT 
        p.brand as value,
        'brand' as type,
        COUNT(*) as product_count
      FROM products p
      WHERE p.brand LIKE ? 
        AND p.status = 1 AND p.visible = 1 AND p.store_authorized = 1
        ${category_id ? 'AND p.category_id = ?' : ''}
      GROUP BY p.brand
      ORDER BY product_count DESC
      LIMIT ?
    `

    // Buscar sugerencias en categorías
    const categorySql = `
      SELECT DISTINCT 
        p.category_name as value,
        'category' as type,
        p.category_id,
        COUNT(*) as product_count
      FROM products p
      WHERE p.category_name LIKE ? 
        AND p.status = 1 AND p.visible = 1 AND p.store_authorized = 1
        ${category_id ? 'AND p.category_id != ?' : ''}
      GROUP BY p.category_name, p.category_id
      ORDER BY product_count DESC
      LIMIT ?
    `

    const likeQuery = `%${searchQuery}%`
    const limitPerType = Math.ceil(parseInt(limit) / 2)

    const brandParams = category_id 
      ? [likeQuery, category_id, limitPerType]
      : [likeQuery, limitPerType]

    const categoryParams = category_id 
      ? [likeQuery, category_id, limitPerType]
      : [likeQuery, limitPerType]

    const [brandResults, categoryResults] = await Promise.all([
      executeQuery(brandSql, brandParams),
      executeQuery(categorySql, categoryParams)
    ])

    const suggestions = [
      ...brandResults.map(row => ({
        value: row.value,
        type: row.type,
        productCount: row.product_count,
        label: `Marca: ${row.value} (${row.product_count} productos)`
      })),
      ...categoryResults.map(row => ({
        value: row.value,
        type: row.type,
        categoryId: row.category_id,
        productCount: row.product_count,
        label: `Categoría: ${row.value} (${row.product_count} productos)`
      }))
    ]
    .sort((a, b) => b.productCount - a.productCount)
    .slice(0, parseInt(limit))

    res.json({
      suggestions,
      query: searchQuery,
      category_id: category_id || null,
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * POST /api/facets/refresh
 * Actualizar facetas pre-calculadas (requiere autenticación de admin)
 */
router.post('/refresh', async (req, res, next) => {
  try {
    // TODO: Agregar middleware de autenticación de admin
    // if (!req.user || !req.user.isAdmin) {
    //   return res.status(403).json({ error: 'Admin access required' })
    // }

    const startTime = Date.now()
    await FacetService.updatePreCalculatedFacets()
    const executionTime = Date.now() - startTime

    res.json({
      success: true,
      message: 'Facets updated successfully',
      executionTime,
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * GET /api/facets/health
 * Estado de salud del sistema de facetas
 */
router.get('/health', cache(60), async (req, res, next) => {
  try {
    // Verificar tabla principal
    const [productCount] = await executeQuery(`
      SELECT COUNT(*) as total 
      FROM products 
      WHERE status = 1 AND visible = 1 AND store_authorized = 1
    `)

    // Verificar tabla de facetas pre-calculadas
    const [facetCount] = await executeQuery(`
      SELECT COUNT(*) as total 
      FROM facet_counts
    `)

    // Verificar última actualización de facetas
    const [lastUpdate] = await executeQuery(`
      SELECT MAX(last_updated) as last_update 
      FROM facet_counts
    `)

    // Estadísticas básicas
    const [stats] = await executeQuery(`
      SELECT 
        COUNT(DISTINCT brand) as brand_count,
        COUNT(DISTINCT store_id) as store_count,
        COUNT(DISTINCT category_id) as category_count,
        AVG(sales_price) as avg_price,
        MIN(sales_price) as min_price,
        MAX(sales_price) as max_price
      FROM products 
      WHERE status = 1 AND visible = 1 AND store_authorized = 1
    `)

    const health = {
      status: 'healthy',
      products: {
        total: productCount.total,
        avgPrice: parseFloat(stats.avg_price),
        priceRange: {
          min: parseFloat(stats.min_price),
          max: parseFloat(stats.max_price)
        }
      },
      facets: {
        preCalculatedCount: facetCount.total,
        lastUpdate: lastUpdate.last_update,
        isStale: lastUpdate.last_update ? 
          (Date.now() - new Date(lastUpdate.last_update).getTime()) > (24 * 60 * 60 * 1000) : 
          true
      },
      dimensions: {
        brands: stats.brand_count,
        stores: stats.store_count,
        categories: stats.category_count
      },
      timestamp: new Date().toISOString()
    }

    // Determinar estado general
    if (productCount.total === 0) {
      health.status = 'error'
    } else if (facetCount.total === 0 || health.facets.isStale) {
      health.status = 'warning'
    }

    res.json(health)
  } catch (error) {
    res.json({
      status: 'error',
      error: error.message,
      timestamp: new Date().toISOString()
    })
  }
})

export { router as facetsRoutes }

/**
 * GET /api/facets/quick
 * Facetas pre-calculadas para carga rápida
 */
router.get('/quick', cache(300), async (req, res, next) => {
  try {
    const { category_id } = req.query
    const facets = await FacetService.getQuickFacets(category_id)
    
    res.json({
      facets,
      category_id: category_id || null,
      type: 'pre-calculated',
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * GET /api/facets/:type
 * Obtener facetas específicas por tipo con conteos dinámicos
 */
router.get('/:type', async (req, res, next) => {
  try {
    const { type } = req.params
    const { 
      q: searchQuery = '',
      category_id: categoryId,
      limit = 50,
      ...filters 
    } = req.query

    // Validar tipos de faceta permitidos
    const allowedTypes = [
      'brands', 'categories', 'priceRanges', 'stores', 
      'fulfillmentTypes', 'ratings', 'features', 'shippingOptions'
    ]
    
    if (!allowedTypes.includes(type)) {
      return res.status(400).json({ 
        error: 'Invalid facet type',
        allowedTypes 
      })
    }

    // Obtener todas las facetas
    const allFacets = await FacetService.getFacets(searchQuery, filters, categoryId)
    
    // Extraer el tipo solicitado y asegurarse de que sea un array
    let requestedFacets = allFacets[type] || []
    
    // Manejar diferentes estructuras de datos según el tipo
    if (type === 'categories' && requestedFacets.level0) {
      // Para categorías, puede tener estructura level0, subcategories, etc.
      requestedFacets = requestedFacets.level0 || requestedFacets.subcategories || []
    } else if (typeof requestedFacets === 'object' && !Array.isArray(requestedFacets)) {
      // Si es un objeto, convertir a array de valores
      requestedFacets = Object.values(requestedFacets).flat()
    }
    
    // Asegurarse de que sea un array
    if (!Array.isArray(requestedFacets)) {
      requestedFacets = []
    }

    // Aplicar límite
    const limitedFacets = requestedFacets.slice(0, parseInt(limit))

    res.json({
      type,
      facets: limitedFacets,
      total: requestedFacets.length,
      category_id: categoryId || null,
      searchQuery: searchQuery || null,
      appliedFilters: allFacets.appliedFilters || [],
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    console.error(`Error getting facets for type ${req.params.type}:`, error)
    next(error)
  }
})

// También agrega esta ruta más específica para categorías
/**
 * GET /api/facets/categories/list
 * Obtener lista simple de categorías (alternativa más directa)
 */
router.get('/categories/list', cache(600), async (req, res, next) => {
  try {
    const { 
      category_id,
      limit = 50,
      q: searchQuery = ''
    } = req.query

    let sql = `
      SELECT DISTINCT
        p.category_id,
        p.category_name,
        p.category_lvl0,
        p.category_lvl1,
        p.category_lvl2,
        COUNT(*) as product_count
      FROM products p
      WHERE p.status = 1 AND p.visible = 1 AND p.store_authorized = 1
        AND p.category_name IS NOT NULL
    `
    const params = []

    if (searchQuery) {
      sql += ` AND (p.name LIKE ? OR p.category_name LIKE ?)`
      const likeQuery = `%${searchQuery}%`
      params.push(likeQuery, likeQuery)
    }

    if (category_id) {
      sql += ` AND p.category_id != ?`
      params.push(category_id)
    }

    sql += `
      GROUP BY p.category_id, p.category_name, p.category_lvl0, p.category_lvl1, p.category_lvl2
      ORDER BY product_count DESC
      LIMIT ?
    `
    params.push(parseInt(limit))

    const results = await executeQuery(sql, params)

    const categories = results.map(row => ({
      id: row.category_id,
      name: row.category_name,
      level0: row.category_lvl0,
      level1: row.category_lvl1,
      level2: row.category_lvl2,
      productCount: row.product_count
    }))

    res.json({
      type: 'categories',
      facets: categories,
      total: categories.length,
      category_id: category_id || null,
      searchQuery: searchQuery || null,
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * GET /api/facets/brands/popular
 * Marcas más populares con estadísticas detalladas
 */
router.get('/brands/popular', cache(600), async (req, res, next) => {
  try {
    const { 
      limit = 20, 
      category_id,
      min_products = 5 
    } = req.query
    
    let sql = `
      SELECT 
        p.brand,
        COUNT(*) as product_count,
        AVG(p.review_rating) as avg_rating,
        COUNT(p.review_rating) as rated_products,
        MIN(p.sales_price) as min_price,
        MAX(p.sales_price) as max_price,
        AVG(p.sales_price) as avg_price,
        SUM(p.total_reviews) as total_reviews,
        COUNT(CASE WHEN p.has_free_shipping = 1 THEN 1 END) as free_shipping_count,
        COUNT(CASE WHEN p.percentage_discount > 0 THEN 1 END) as discounted_count
      FROM products p
      WHERE p.status = 1 AND p.visible = 1 AND p.store_authorized = 1
        AND p.brand IS NOT NULL
    `
    const params = []
    
    if (category_id) {
      sql += ` AND p.category_id = ?`
      params.push(category_id)
    }
    
    sql += `
      GROUP BY p.brand
      HAVING product_count >= ?
      ORDER BY product_count DESC, avg_rating DESC
      LIMIT ?
    `
    params.push(parseInt(min_products), parseInt(limit))
    
    const results = await executeQuery(sql, params)
    
    res.json({
      popularBrands: results.map(row => ({
        brand: row.brand,
        productCount: row.product_count,
        avgRating: parseFloat(row.avg_rating) || 0,
        ratedProducts: row.rated_products,
        priceRange: {
          min: parseFloat(row.min_price),
          max: parseFloat(row.max_price),
          avg: parseFloat(row.avg_price)
        },
        totalReviews: row.total_reviews,
        freeShippingPercentage: Math.round((row.free_shipping_count / row.product_count) * 100),
        discountedPercentage: Math.round((row.discounted_count / row.product_count) * 100)
      })),
      category_id: category_id || null,
      criteria: {
        minProducts: parseInt(min_products),
        limit: parseInt(limit)
      },
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * GET /api/facets/price-ranges/dynamic
 * Rangos de precio calculados dinámicamente
 */
router.get('/price-ranges/dynamic', async (req, res, next) => {
  try {
    const { 
      category_id,
      q: searchQuery = '',
      brand,
      store_id,
      ...otherFilters
    } = req.query

    // Construir filtros excluyendo precio
    const filters = { brand, store_id, ...otherFilters }
    delete filters.min_price
    delete filters.max_price

    const facets = await FacetService.getFacets(searchQuery, filters, category_id)
    
    res.json({
      priceRanges: facets.priceRanges,
      totalProducts: facets.totalProducts,
      category_id: category_id || null,
      searchQuery: searchQuery || null,
      appliedFilters: facets.appliedFilters.filter(f => f.type !== 'price_range'),
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})