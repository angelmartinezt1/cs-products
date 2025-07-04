import { Router } from 'express'
import { executeQuery } from '../config/database.js'
import { cache } from '../middleware/cache.js'
import { SearchService } from '../services/searchService.js'

const router = Router()

/**
 * GET /api/categories
 * Obtener todas las categorías con conteos de productos
 */
router.get('/', cache(3600), async (req, res, next) => {
  try {
    const sql = `
      SELECT 
        category_id,
        category_name,
        category_lvl0,
        category_lvl1,
        category_lvl2,
        category_path,
        COUNT(*) as product_count,
        MIN(sales_price) as min_price,
        MAX(sales_price) as max_price,
        AVG(sales_price) as avg_price,
        AVG(review_rating) as avg_rating
      FROM products
      WHERE status = 1 AND visible = 1 AND store_authorized = 1
        AND category_id IS NOT NULL
      GROUP BY category_id, category_name, category_lvl0, category_lvl1, category_lvl2, category_path
      HAVING product_count > 0
      ORDER BY product_count DESC
    `
    
    const results = await executeQuery(sql)
    
    const categories = results.map(row => ({
      id: row.category_id,
      name: row.category_name,
      path: row.category_path,
      levels: {
        lvl0: row.category_lvl0,
        lvl1: row.category_lvl1,
        lvl2: row.category_lvl2
      },
      stats: {
        productCount: row.product_count,
        priceRange: {
          min: parseFloat(row.min_price),
          max: parseFloat(row.max_price),
          avg: parseFloat(row.avg_price)
        },
        avgRating: parseFloat(row.avg_rating)
      }
    }))
    
    res.json({
      categories,
      total: categories.length,
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * GET /api/categories/:categoryId
 * Obtener detalles de una categoría específica
 */
router.get('/:categoryId', cache(1800), async (req, res, next) => {
  try {
    const { categoryId } = req.params
    
    // Validar que categoryId sea un número
    if (isNaN(categoryId)) {
      return res.status(400).json({ error: 'Invalid category ID' })
    }
    
    // Obtener información de la categoría
    const categorySql = `
      SELECT 
        category_id,
        category_name,
        category_lvl0,
        category_lvl1,
        category_lvl2,
        category_path,
        COUNT(*) as product_count,
        MIN(sales_price) as min_price,
        MAX(sales_price) as max_price,
        AVG(sales_price) as avg_price,
        AVG(review_rating) as avg_rating
      FROM products
      WHERE category_id = ? 
        AND status = 1 AND visible = 1 AND store_authorized = 1
      GROUP BY category_id, category_name, category_lvl0, category_lvl1, category_lvl2, category_path
    `
    
    const [categoryInfo] = await executeQuery(categorySql, [categoryId])
    
    if (!categoryInfo) {
      return res.status(404).json({ error: 'Category not found' })
    }
    
    // Obtener subcategorías (si las hay)
    const subcategoriesSql = `
      SELECT DISTINCT
        category_lvl2 as name,
        COUNT(*) as product_count
      FROM products
      WHERE category_lvl1 = ? 
        AND category_lvl2 IS NOT NULL
        AND category_lvl2 != category_lvl1
        AND status = 1 AND visible = 1 AND store_authorized = 1
      GROUP BY category_lvl2
      ORDER BY product_count DESC
      LIMIT 20
    `
    
    const subcategories = await executeQuery(subcategoriesSql, [categoryInfo.category_lvl1])
    
    // Obtener marcas top en esta categoría
    const brandsSql = `
      SELECT 
        brand,
        COUNT(*) as product_count,
        AVG(review_rating) as avg_rating
      FROM products
      WHERE category_id = ?
        AND brand IS NOT NULL
        AND status = 1 AND visible = 1 AND store_authorized = 1
      GROUP BY brand
      ORDER BY product_count DESC
      LIMIT 15
    `
    
    const topBrands = await executeQuery(brandsSql, [categoryId])
    
    // Obtener productos destacados de la categoría
    const featuredSql = `
      SELECT 
        id, name, sales_price, main_image, review_rating, total_reviews,
        brand, store_name, has_free_shipping, percentage_discount
      FROM products
      WHERE category_id = ?
        AND status = 1 AND visible = 1 AND store_authorized = 1
        AND stock > 0
      ORDER BY (review_rating * total_reviews) DESC, total_reviews DESC
      LIMIT 12
    `
    
    const featuredProducts = await executeQuery(featuredSql, [categoryId])
    
    res.json({
      category: {
        id: categoryInfo.category_id,
        name: categoryInfo.category_name,
        path: categoryInfo.category_path,
        levels: {
          lvl0: categoryInfo.category_lvl0,
          lvl1: categoryInfo.category_lvl1,
          lvl2: categoryInfo.category_lvl2
        },
        stats: {
          productCount: categoryInfo.product_count,
          priceRange: {
            min: parseFloat(categoryInfo.min_price),
            max: parseFloat(categoryInfo.max_price),
            avg: parseFloat(categoryInfo.avg_price)
          },
          avgRating: parseFloat(categoryInfo.avg_rating)
        }
      },
      subcategories: subcategories.map(sub => ({
        name: sub.name,
        productCount: sub.product_count
      })),
      topBrands: topBrands.map(brand => ({
        name: brand.brand,
        productCount: brand.product_count,
        avgRating: parseFloat(brand.avg_rating)
      })),
      featuredProducts: featuredProducts.map(product => ({
        id: product.id,
        name: product.name,
        price: parseFloat(product.sales_price),
        image: product.main_image,
        rating: parseFloat(product.review_rating),
        reviewCount: product.total_reviews,
        brand: product.brand,
        store: product.store_name,
        freeShipping: product.has_free_shipping === 1,
        discount: product.percentage_discount
      })),
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * GET /api/categories/:categoryId/products
 * Obtener productos de una categoría con filtros y paginación
 */
router.get('/:categoryId/products', async (req, res, next) => {
  try {
    const { categoryId } = req.params
    
    // Validar que categoryId sea un número
    if (isNaN(categoryId)) {
      return res.status(400).json({ error: 'Invalid category ID' })
    }
    
    // Validar que la categoría existe
    const categoryExists = await executeQuery(
      'SELECT 1 FROM products WHERE category_id = ? LIMIT 1',
      [categoryId]
    )
    
    if (!categoryExists.length) {
      return res.status(404).json({ error: 'Category not found' })
    }
    
    // Usar SearchService con filtro de categoría
    const searchParams = {
      query: req.query.q || '',
      page: parseInt(req.query.page) || 1,
      limit: Math.min(parseInt(req.query.limit) || 20, 100),
      sortBy: req.query.sort || 'relevance',
      sortOrder: req.query.order || 'desc',
      facets: req.query.facets !== 'false',
      filters: {
        category_id: categoryId,
        ...extractFilters(req.query)
      }
    }
    
    const results = await SearchService.searchProducts(searchParams)
    
    res.json({
      ...results,
      categoryId: parseInt(categoryId)
    })
  } catch (error) {
    next(error)
  }
})

/**
 * GET /api/categories/tree
 * Obtener árbol completo de categorías
 */
router.get('/tree', cache(3600), async (req, res, next) => {
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
    
    // Construir árbol jerárquico
    const tree = {}
    
    results.forEach(row => {
      const lvl0 = row.category_lvl0
      const lvl1 = row.category_lvl1
      const lvl2 = row.category_lvl2
      
      // Nivel 0
      if (!tree[lvl0]) {
        tree[lvl0] = {
          name: lvl0,
          productCount: 0,
          children: {}
        }
      }
      tree[lvl0].productCount += row.product_count
      
      // Nivel 1
      if (lvl1 && lvl1 !== lvl0) {
        if (!tree[lvl0].children[lvl1]) {
          tree[lvl0].children[lvl1] = {
            name: lvl1,
            productCount: 0,
            children: {}
          }
        }
        tree[lvl0].children[lvl1].productCount += row.product_count
        
        // Nivel 2
        if (lvl2 && lvl2 !== lvl1) {
          if (!tree[lvl0].children[lvl1].children[lvl2]) {
            tree[lvl0].children[lvl1].children[lvl2] = {
              name: lvl2,
              productCount: row.product_count
            }
          } else {
            tree[lvl0].children[lvl1].children[lvl2].productCount += row.product_count
          }
        }
      }
    })
    
    // Convertir a formato de array y ordenar
    const categoryTree = Object.values(tree)
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
      tree: categoryTree,
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

/**
 * GET /api/categories/search
 * Buscar categorías por nombre
 */
router.get('/search', async (req, res, next) => {
  try {
    const { q: query, limit = 10 } = req.query
    
    if (!query || query.length < 2) {
      return res.json({ categories: [] })
    }
    
    const sql = `
      SELECT DISTINCT
        category_id,
        category_name,
        category_path,
        category_lvl0,
        category_lvl1,
        category_lvl2,
        COUNT(*) as product_count
      FROM products
      WHERE (
        category_name LIKE ? OR 
        category_lvl0 LIKE ? OR 
        category_lvl1 LIKE ? OR 
        category_lvl2 LIKE ?
      )
      AND status = 1 AND visible = 1 AND store_authorized = 1
      GROUP BY category_id, category_name, category_path, category_lvl0, category_lvl1, category_lvl2
      ORDER BY product_count DESC
      LIMIT ?
    `
    
    const likeQuery = `%${query}%`
    const results = await executeQuery(sql, [
      likeQuery, likeQuery, likeQuery, likeQuery, parseInt(limit)
    ])
    
    const categories = results.map(row => ({
      id: row.category_id,
      name: row.category_name,
      path: row.category_path,
      levels: {
        lvl0: row.category_lvl0,
        lvl1: row.category_lvl1,
        lvl2: row.category_lvl2
      },
      productCount: row.product_count
    }))
    
    res.json({
      categories,
      query,
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    next(error)
  }
})

// Helper function para extraer filtros
function extractFilters(query) {
  const filters = {}
  
  const simpleFilters = [
    'brand', 'store_id', 'fulfillment_type', 'min_price', 'max_price', 
    'min_rating', 'free_shipping', 'digital', 'has_discount'
  ]
  
  simpleFilters.forEach(filter => {
    if (query[filter] !== undefined) {
      if (typeof query[filter] === 'string' && query[filter].includes(',')) {
        filters[filter] = query[filter].split(',').map(v => v.trim())
      } else {
        filters[filter] = query[filter]
      }
    }
  })
  
  return filters
}

export { router as categoriesRoutes }
