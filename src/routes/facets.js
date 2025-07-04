import { Router } from 'express'
import { executeQuery } from '../config/database.js'
import { cache } from '../middleware/cache.js'

const router = Router()

/**
 * GET /api/facets
 * Obtener todas las facetas disponibles (desde tabla pre-calculada)
 */
router.get('/', cache(300), async (req, res, next) => {
  try {
    const { category_id } = req.query
    
    let sql = `
      SELECT facet_type, facet_value, facet_count
      FROM facet_counts 
      WHERE facet_count > 0
    `
    const params = []
    
    if (category_id) {
      sql += ` AND (category_id = ? OR category_id IS NULL)`
      params.push(category_id)
    } else {
      sql += ` AND category_id IS NULL`
    }
    
    sql += ` ORDER BY facet_type, facet_count DESC`
    
    const results = await executeQuery(sql, params)
    
    // Agrupar por tipo de faceta
    const facets = {}
    results.forEach(row => {
      if (!facets[row.facet_type]) {
        facets[row.facet_type] = []
      }
      facets[row.facet_type].push({
        value: row.facet_value,
        count: row.facet_count
      })
    })
    
    res.json({
      facets,
      category_id: category_id || null,
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * GET /api/facets/:type
 * Obtener facetas específicas por tipo
 */
router.get('/:type', cache(300), async (req, res, next) => {
  try {
    const { type } = req.params
    const { category_id, limit = 50 } = req.query
    
    let sql = `
      SELECT facet_value, facet_count
      FROM facet_counts 
      WHERE facet_type = ? AND facet_count > 0
    `
    const params = [type]
    
    if (category_id) {
      sql += ` AND (category_id = ? OR category_id IS NULL)`
      params.push(category_id)
    } else {
      sql += ` AND category_id IS NULL`
    }
    
    sql += ` ORDER BY facet_count DESC LIMIT ?`
    params.push(parseInt(limit))
    
    const results = await executeQuery(sql, params)
    
    res.json({
      type,
      facets: results.map(row => ({
        value: row.facet_value,
        count: row.facet_count
      })),
      category_id: category_id || null,
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * GET /api/facets/brands/popular
 * Marcas más populares
 */
router.get('/brands/popular', cache(600), async (req, res, next) => {
  try {
    const { limit = 20 } = req.query
    
    const sql = `
      SELECT 
        p.brand,
        COUNT(*) as product_count,
        AVG(p.review_rating) as avg_rating,
        MIN(p.sales_price) as min_price,
        MAX(p.sales_price) as max_price
      FROM products p
      WHERE p.status = 1 AND p.visible = 1 AND p.store_authorized = 1
        AND p.brand IS NOT NULL
      GROUP BY p.brand
      HAVING product_count >= 5
      ORDER BY product_count DESC, avg_rating DESC
      LIMIT ?
    `
    
    const results = await executeQuery(sql, [parseInt(limit)])
    
    res.json({
      popular_brands: results.map(row => ({
        brand: row.brand,
        productCount: row.product_count,
        avgRating: parseFloat(row.avg_rating),
        priceRange: {
          min: parseFloat(row.min_price),
          max: parseFloat(row.max_price)
        }
      })),
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * GET /api/facets/categories/hierarchy
 * Jerarquía de categorías
 */
router.get('/categories/hierarchy', cache(3600), async (req, res, next) => {
  try {
    const sql = `
      SELECT 
        category_lvl0,
        category_lvl1,
        category_lvl2,
        COUNT(*) as product_count
      FROM products
      WHERE status = 1 AND visible = 1 AND store_authorized = 1
        AND category_lvl0 IS NOT NULL
      GROUP BY category_lvl0, category_lvl1, category_lvl2
      ORDER BY category_lvl0, category_lvl1, category_lvl2
    `
    
    const results = await executeQuery(sql)
    
    // Construir jerarquía
    const hierarchy = {}
    
    results.forEach(row => {
      const lvl0 = row.category_lvl0
      const lvl1 = row.category_lvl1
      const lvl2 = row.category_lvl2
      
      if (!hierarchy[lvl0]) {
        hierarchy[lvl0] = {
          name: lvl0,
          productCount: 0,
          children: {}
        }
      }
      
      hierarchy[lvl0].productCount += row.product_count
      
      if (lvl1) {
        if (!hierarchy[lvl0].children[lvl1]) {
          hierarchy[lvl0].children[lvl1] = {
            name: lvl1,
            productCount: 0,
            children: {}
          }
        }
        
        hierarchy[lvl0].children[lvl1].productCount += row.product_count
        
        if (lvl2) {
          if (!hierarchy[lvl0].children[lvl1].children[lvl2]) {
            hierarchy[lvl0].children[lvl1].children[lvl2] = {
              name: lvl2,
              productCount: 0
            }
          }
          
          hierarchy[lvl0].children[lvl1].children[lvl2].productCount += row.product_count
        }
      }
    })
    
    // Convertir a array y ordenar
    const categoryTree = Object.values(hierarchy)
      .map(lvl0 => ({
        ...lvl0,
        children: Object.values(lvl0.children)
          .map(lvl1 => ({
            ...lvl1,
            children: Object.values(lvl1.children)
              .sort((a, b) => b.productCount - a.productCount)
          }))
          .sort((a, b) => b.productCount - a.productCount)
      }))
      .sort((a, b) => b.productCount - a.productCount)
    
    res.json({
      categories: categoryTree,
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * GET /api/facets/price-ranges
 * Rangos de precio dinámicos
 */
router.get('/price-ranges', cache(600), async (req, res, next) => {
  try {
    const { category_id } = req.query
    
    let sql = `
      SELECT 
        MIN(sales_price) as min_price,
        MAX(sales_price) as max_price,
        AVG(sales_price) as avg_price,
        COUNT(*) as total_products
      FROM products
      WHERE status = 1 AND visible = 1 AND store_authorized = 1
        AND sales_price > 0
    `
    const params = []
    
    if (category_id) {
      sql += ` AND category_id = ?`
      params.push(category_id)
    }
    
    const [stats] = await executeQuery(sql, params)
    
    if (!stats) {
      return res.json({ ranges: [] })
    }
    
    // Crear rangos dinámicos basados en la distribución
    const ranges = generatePriceRanges(
      stats.min_price, 
      stats.max_price, 
      stats.avg_price
    )
    
    // Obtener conteos para cada rango
    const rangeQueries = ranges.map(range => 
      `SELECT 
        COUNT(*) as count,
        '${range.label}' as label,
        ${range.min} as min_price,
        ${range.max || 999999999} as max_price
       FROM products 
       WHERE status = 1 AND visible = 1 AND store_authorized = 1
         AND sales_price >= ${range.min}
         ${range.max ? `AND sales_price <= ${range.max}` : ''}
         ${category_id ? `AND category_id = ${category_id}` : ''}`
    )
    
    const rangeResults = await Promise.all(
      rangeQueries.map(query => executeQuery(query))
    )
    
    const priceRanges = rangeResults
      .map(([result]) => result)
      .filter(range => range.count > 0)
    
    res.json({
      ranges: priceRanges,
      stats: {
        min: parseFloat(stats.min_price),
        max: parseFloat(stats.max_price),
        avg: parseFloat(stats.avg_price),
        total: stats.total_products
      },
      category_id: category_id || null,
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * POST /api/facets/refresh
 * Actualizar facetas pre-calculadas (admin only)
 */
router.post('/refresh', async (req, res, next) => {
  try {
    // En producción, agregar autenticación de admin aquí
    
    await executeQuery('CALL UpdateAllFacets()')
    
    res.json({
      success: true,
      message: 'Facets updated successfully',
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

// Helper function para generar rangos de precio dinámicos
function generatePriceRanges(minPrice, maxPrice, avgPrice) {
  const ranges = []
  
  if (maxPrice <= 1000) {
    // Productos baratos
    ranges.push(
      { label: 'Menos de $200', min: 0, max: 199 },
      { label: '$200 - $499', min: 200, max: 499 },
      { label: '$500 - $999', min: 500, max: 999 },
      { label: '$1,000+', min: 1000 }
    )
  } else if (maxPrice <= 10000) {
    // Rango medio
    ranges.push(
      { label: 'Menos de $1,000', min: 0, max: 999 },
      { label: '$1,000 - $2,999', min: 1000, max: 2999 },
      { label: '$3,000 - $5,999', min: 3000, max: 5999 },
      { label: '$6,000 - $9,999', min: 6000, max: 9999 },
      { label: '$10,000+', min: 10000 }
    )
  } else {
    // Productos caros
    ranges.push(
      { label: 'Menos de $5,000', min: 0, max: 4999 },
      { label: '$5,000 - $14,999', min: 5000, max: 14999 },
      { label: '$15,000 - $29,999', min: 15000, max: 29999 },
      { label: '$30,000 - $49,999', min: 30000, max: 49999 },
      { label: '$50,000+', min: 50000 }
    )
  }
  
  return ranges
}

export { router as facetsRoutes }
