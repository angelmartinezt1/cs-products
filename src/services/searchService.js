import { executeQuery } from '../config/database.js'

export class SearchService {
  
  // Búsqueda principal de productos
  static async searchProducts(params) {
    const {
      query = '',
      page = 1,
      limit = 20,
      sortBy = 'relevance',
      sortOrder = 'desc',
      filters = {},
      facets = true
    } = params

    const offset = (page - 1) * limit
    
    try {
      // Construir la consulta base
      const { sql, countSql, queryParams } = this.buildSearchQuery(query, filters, sortBy, sortOrder, limit, offset)
      
      // Ejecutar búsqueda y conteo en paralelo
      const [products, totalResults, facetResults] = await Promise.all([
        executeQuery(sql, queryParams),
        this.getSearchCount(countSql, queryParams.slice(0, -2)), // Sin LIMIT y OFFSET
        facets ? this.getFacets(query, filters) : Promise.resolve([])
      ])

      // Enriquecer productos con datos adicionales
      const enrichedProducts = await this.enrichProducts(products)

      return {
        products: enrichedProducts,
        pagination: {
          page: parseInt(page),
          limit: parseInt(limit),
          total: totalResults[0]?.total || 0,
          totalPages: Math.ceil((totalResults[0]?.total || 0) / limit)
        },
        facets: facetResults,
        query: query,
        executionTime: Date.now()
      }
    } catch (error) {
      console.error('Search error:', error)
      throw new Error(`Search failed: ${error.message}`)
    }
  }

  static buildSearchQuery(query, filters, sortBy, sortOrder, limit, offset) {
    let sql = `
      SELECT 
        p.id,
        p.name,
        p.description,
        p.short_description,
        p.sku,
        p.brand,
        p.sales_price,
        p.list_price,
        p.shipping_cost,
        p.percentage_discount,
        p.stock,
        p.category_id,
        p.category_name,
        p.category_path,
        p.store_id,
        p.store_name,
        p.store_logo,
        p.store_rating,
        p.digital,
        p.big_ticket,
        p.back_order,
        p.is_store_pickup,
        p.super_express,
        p.shipping_days,
        p.review_rating,
        p.total_reviews,
        p.main_image,
        p.thumbnail,
        p.fulfillment_type,
        p.has_free_shipping,
        p.relevance_score
      FROM products p
      WHERE p.status = 1 
        AND p.visible = 1 
        AND p.store_authorized = 1
        AND p.stock > 0
    `
  
    const queryParams = []
    const whereConditions = []
  
    // Búsqueda por texto
    if (query && query.trim()) {
      const likeQuery = `%${query.trim()}%`
      whereConditions.push(`(
        p.name LIKE ? OR 
        p.brand LIKE ? OR 
        p.search_text LIKE ?
      )`)
      queryParams.push(likeQuery, likeQuery, likeQuery)
    }
  
    // Aplicar filtros
    const filterConditions = this.buildFilterConditions(filters, queryParams)
    whereConditions.push(...filterConditions)
  
    // Agregar condiciones WHERE
    if (whereConditions.length > 0) {
      sql += ` AND (${whereConditions.join(' AND ')})`
    }
  
    // SQL para conteo (sin ORDER BY ni LIMIT)
    const countSql = sql.replace(/SELECT[\s\S]*?FROM/, 'SELECT COUNT(*) as total FROM')
  
    // Ordenamiento
    sql += this.buildOrderBy(sortBy, sortOrder, query)
  
    // Paginación
    sql += ` LIMIT ? OFFSET ?`
    queryParams.push(parseInt(limit), parseInt(offset))
  
    console.log('🔍 SQL:', sql) // Debug
    console.log('📊 Params:', queryParams) // Debug
  
    return { sql, countSql, queryParams }
  }

  // Determinar si es una búsqueda simple
  static isSimpleQuery(query) {
    // Si es una sola palabra o palabras simples, usar LIKE
    return query.split(' ').length <= 2 && !/[+\-*"~<>()]/.test(query)
  }

  // Construir condiciones de filtros
  static buildFilterConditions(filters, queryParams) {
    const conditions = []

    // Filtro por categoría
    if (filters.category_id) {
      conditions.push('p.category_id = ?')
      queryParams.push(filters.category_id)
    }

    if (filters.category_lvl0) {
      conditions.push('p.category_lvl0 = ?')
      queryParams.push(filters.category_lvl0)
    }

    if (filters.category_lvl1) {
      conditions.push('p.category_lvl1 = ?')
      queryParams.push(filters.category_lvl1)
    }

    // Filtro por marca
    if (filters.brand) {
      if (Array.isArray(filters.brand)) {
        conditions.push(`p.brand IN (${filters.brand.map(() => '?').join(',')})`)
        queryParams.push(...filters.brand)
      } else {
        conditions.push('p.brand = ?')
        queryParams.push(filters.brand)
      }
    }

    // Filtro por tienda
    if (filters.store_id) {
      if (Array.isArray(filters.store_id)) {
        conditions.push(`p.store_id IN (${filters.store_id.map(() => '?').join(',')})`)
        queryParams.push(...filters.store_id)
      } else {
        conditions.push('p.store_id = ?')
        queryParams.push(filters.store_id)
      }
    }

    // Filtro por rango de precio
    if (filters.min_price) {
      conditions.push('p.sales_price >= ?')
      queryParams.push(filters.min_price)
    }

    if (filters.max_price) {
      conditions.push('p.sales_price <= ?')
      queryParams.push(filters.max_price)
    }

    // Filtro por envío gratis
    if (filters.free_shipping === 'true' || filters.free_shipping === true) {
      conditions.push('p.has_free_shipping = 1')
    }

    // Filtro por fulfillment
    if (filters.fulfillment_type) {
      conditions.push('p.fulfillment_type = ?')
      queryParams.push(filters.fulfillment_type)
    }

    // Filtro por rating mínimo
    if (filters.min_rating) {
      conditions.push('p.review_rating >= ?')
      queryParams.push(filters.min_rating)
    }

    // Filtro por productos digitales
    if (filters.digital === 'true' || filters.digital === true) {
      conditions.push('p.digital = 1')
    }

    // Filtro por descuentos
    if (filters.has_discount === 'true' || filters.has_discount === true) {
      conditions.push('p.percentage_discount > 0')
    }

    return conditions
  }

  // Construir ORDER BY
  static buildOrderBy(sortBy, sortOrder, query) {
    const order = sortOrder.toLowerCase() === 'asc' ? 'ASC' : 'DESC'
    
    switch (sortBy) {
      case 'price':
        return ` ORDER BY p.sales_price ${order}`
      case 'rating':
        return ` ORDER BY p.review_rating ${order}, p.total_reviews DESC`
      case 'newest':
        return ` ORDER BY p.created_at DESC`
      case 'name':
        return ` ORDER BY p.name ${order}`
      case 'reviews':
        return ` ORDER BY p.total_reviews ${order}`
      case 'discount':
        return ` ORDER BY p.percentage_discount ${order}`
      case 'relevance':
      default:
        if (query.trim()) {
          return ` ORDER BY p.relevance_score DESC, p.review_rating DESC, p.total_reviews DESC`
        }
        return ` ORDER BY p.relevance_score DESC, p.sales_price ASC`
    }
  }

  // Obtener conteo total
  static async getSearchCount(countSql, queryParams) {
    return await executeQuery(countSql, queryParams)
  }

  // Enriquecer productos con datos adicionales
  static async enrichProducts(products) {
    if (!products.length) return products

    const productIds = products.map(p => p.id)
    const placeholders = productIds.map(() => '?').join(',')

    // Obtener imágenes adicionales
    const images = await executeQuery(`
      SELECT product_id, image_url, thumbnail_url, image_order
      FROM product_images 
      WHERE product_id IN (${placeholders})
      ORDER BY product_id, image_order
    `, productIds)

    // Obtener variaciones
    const variations = await executeQuery(`
      SELECT product_id, COUNT(*) as variation_count, SUM(stock) as total_stock
      FROM product_variations 
      WHERE product_id IN (${placeholders})
      GROUP BY product_id
    `, productIds)

    // Mapear datos adicionales
    const imagesMap = {}
    const variationsMap = {}

    images.forEach(img => {
      if (!imagesMap[img.product_id]) imagesMap[img.product_id] = []
      imagesMap[img.product_id].push({
        url: img.image_url,
        thumbnail: img.thumbnail_url,
        order: img.image_order
      })
    })

    variations.forEach(v => {
      variationsMap[v.product_id] = {
        count: v.variation_count,
        totalStock: v.total_stock
      }
    })

    // Enriquecer productos
    return products.map(product => ({
      ...product,
      images: imagesMap[product.id] || [],
      variations: variationsMap[product.id] || { count: 0, totalStock: 0 },
      // Campos calculados
      hasDiscount: product.percentage_discount > 0,
      finalPrice: product.sales_price,
      savings: product.list_price ? product.list_price - product.sales_price : 0,
      freeShipping: product.has_free_shipping === 1,
      inStock: product.stock > 0
    }))
  }

  // Obtener facetas
  static async getFacets(query = '', filters = {}) {
    try {
      // Construir query base para facetas (sin paginación)
      const { sql: baseSql, queryParams: baseParams } = this.buildSearchQuery(
        query, 
        filters, 
        'relevance', 
        'desc', 
        999999, 
        0
      )

      // Remover SELECT y ORDER BY para reutilizar WHERE
      const whereClause = baseSql
        .replace(/SELECT[\s\S]*?FROM products p/, '')
        .replace(/ORDER BY[\s\S]*/, '')
        .replace(/LIMIT[\s\S]*/, '')

      const whereParams = baseParams.slice(0, -2) // Sin LIMIT y OFFSET

      // Ejecutar todas las consultas de facetas en paralelo
      const [
        brands,
        categories,
        priceRanges,
        stores,
        fulfillmentTypes,
        ratings
      ] = await Promise.all([
        this.getBrandFacets(whereClause, whereParams, filters),
        this.getCategoryFacets(whereClause, whereParams, filters),
        this.getPriceFacets(whereClause, whereParams, filters),
        this.getStoreFacets(whereClause, whereParams, filters),
        this.getFulfillmentFacets(whereClause, whereParams, filters),
        this.getRatingFacets(whereClause, whereParams, filters)
      ])

      return {
        brands,
        categories,
        priceRanges,
        stores,
        fulfillmentTypes,
        ratings
      }
    } catch (error) {
      console.error('Facets error:', error)
      return {}
    }
  }

  // Facetas por marca
  static async getBrandFacets(whereClause, whereParams, filters) {
    if (filters.brand) return [] // No mostrar si ya está filtrado

    const sql = `
      SELECT p.brand as value, COUNT(*) as count
      FROM products p
      ${whereClause}
      AND p.brand IS NOT NULL
      GROUP BY p.brand
      ORDER BY count DESC
      LIMIT 20
    `
    return await executeQuery(sql, whereParams)
  }

  // Facetas por categoría
  static async getCategoryFacets(whereClause, whereParams, filters) {
    const sql = `
      SELECT 
        p.category_lvl0 as value, 
        COUNT(*) as count,
        'lvl0' as level
      FROM products p
      ${whereClause}
      AND p.category_lvl0 IS NOT NULL
      GROUP BY p.category_lvl0
      ORDER BY count DESC
      LIMIT 10
    `
    return await executeQuery(sql, whereParams)
  }

  // Facetas por precio
  static async getPriceFacets(whereClause, whereParams, filters) {
    if (filters.min_price || filters.max_price) return []

    const sql = `
      SELECT 
        CASE 
          WHEN p.sales_price < 1000 THEN '0-999'
          WHEN p.sales_price < 5000 THEN '1000-4999'
          WHEN p.sales_price < 10000 THEN '5000-9999'
          WHEN p.sales_price < 25000 THEN '10000-24999'
          WHEN p.sales_price < 50000 THEN '25000-49999'
          ELSE '50000+'
        END as value,
        COUNT(*) as count
      FROM products p
      ${whereClause}
      GROUP BY value
      ORDER BY MIN(p.sales_price)
    `
    return await executeQuery(sql, whereParams)
  }

  // Facetas por tienda
  static async getStoreFacets(whereClause, whereParams, filters) {
    if (filters.store_id) return []

    const sql = `
      SELECT 
        p.store_name as value,
        p.store_id as id,
        COUNT(*) as count,
        ROUND(AVG(p.store_rating), 1) as rating
      FROM products p
      ${whereClause}
      GROUP BY p.store_id, p.store_name
      ORDER BY count DESC
      LIMIT 15
    `
    return await executeQuery(sql, whereParams)
  }

  // Facetas por fulfillment
  static async getFulfillmentFacets(whereClause, whereParams, filters) {
    if (filters.fulfillment_type) return []

    const sql = `
      SELECT 
        p.fulfillment_type as value,
        COUNT(*) as count
      FROM products p
      ${whereClause}
      GROUP BY p.fulfillment_type
      ORDER BY count DESC
    `
    return await executeQuery(sql, whereParams)
  }

  // Facetas por rating
  static async getRatingFacets(whereClause, whereParams, filters) {
    const sql = `
      SELECT 
        CASE 
          WHEN p.review_rating >= 4.5 THEN '4.5+'
          WHEN p.review_rating >= 4.0 THEN '4.0+'
          WHEN p.review_rating >= 3.5 THEN '3.5+'
          WHEN p.review_rating >= 3.0 THEN '3.0+'
          ELSE '2.9-'
        END as value,
        COUNT(*) as count
      FROM products p
      ${whereClause}
      AND p.review_rating IS NOT NULL
      GROUP BY value
      ORDER BY MIN(p.review_rating) DESC
    `
    return await executeQuery(sql, whereParams)
  }

  // Búsqueda de autocompletado
  static async getAutocompleteSuggestions(query, limit = 10) {
    if (!query || query.length < 2) return []

    const likeQuery = `${query}%`
    
    const sql = `
      (SELECT DISTINCT name as suggestion, 'product' as type, 1 as priority
       FROM products 
       WHERE name LIKE ? AND status = 1 AND visible = 1
       ORDER BY name LIMIT ?)
      UNION
      (SELECT DISTINCT brand as suggestion, 'brand' as type, 2 as priority
       FROM products 
       WHERE brand LIKE ? AND status = 1 AND visible = 1
       ORDER BY brand LIMIT ?)
      UNION  
      (SELECT DISTINCT category_name as suggestion, 'category' as type, 3 as priority
       FROM products 
       WHERE category_name LIKE ? AND status = 1 AND visible = 1
       ORDER BY category_name LIMIT ?)
      ORDER BY priority, suggestion
      LIMIT ?
    `

    return await executeQuery(sql, [
      likeQuery, Math.ceil(limit * 0.6),
      likeQuery, Math.ceil(limit * 0.2), 
      likeQuery, Math.ceil(limit * 0.2),
      limit
    ])
  }
  static async simpleSearch(query = '', page = 1, limit = 20, filters = {}) {
    try {
      let sql = `
        SELECT 
          p.id,
          p.name,
          p.description,
          p.brand,
          p.sales_price,
          p.list_price,
          p.stock,
          p.category_name,
          p.store_name,
          p.review_rating,
          p.total_reviews,
          p.main_image,
          p.relevance_score,
          p.has_free_shipping,
          p.fulfillment_type,
          p.percentage_discount
        FROM products p
        WHERE p.status = 1 
          AND p.visible = 1 
          AND p.store_authorized = 1
          AND p.stock > 0
      `
      
      const params = []
      
      // Búsqueda por texto
      if (query && query.trim()) {
        sql += ` AND (p.name LIKE ? OR p.brand LIKE ? OR p.description LIKE ?)`
        const likeQuery = `%${query.trim()}%`
        params.push(likeQuery, likeQuery, likeQuery)
      }
      
      // Filtros básicos
      if (filters.brand) {
        sql += ` AND p.brand = ?`
        params.push(filters.brand)
      }
      
      if (filters.category_id) {
        sql += ` AND p.category_id = ?`
        params.push(Number(filters.category_id))
      }
      
      if (filters.min_price) {
        sql += ` AND p.sales_price >= ?`
        params.push(Number(filters.min_price))
      }
      
      if (filters.max_price) {
        sql += ` AND p.sales_price <= ?`
        params.push(Number(filters.max_price))
      }
      
      if (filters.free_shipping === 'true') {
        sql += ` AND p.has_free_shipping = 1`
      }
      
      if (filters.fulfillment_type) {
        sql += ` AND p.fulfillment_type = ?`
        params.push(filters.fulfillment_type)
      }
      
      if (filters.min_rating) {
        sql += ` AND p.review_rating >= ?`
        params.push(Number(filters.min_rating))
      }
      
      // Ordenamiento
      const sortBy = filters.sort || 'relevance'
      switch (sortBy) {
        case 'price':
          sql += ` ORDER BY p.sales_price ${filters.order === 'desc' ? 'DESC' : 'ASC'}`
          break
        case 'rating':
          sql += ` ORDER BY p.review_rating DESC, p.total_reviews DESC`
          break
        case 'newest':
          sql += ` ORDER BY p.created_at DESC`
          break
        case 'name':
          sql += ` ORDER BY p.name ${filters.order === 'desc' ? 'DESC' : 'ASC'}`
          break
        case 'discount':
          sql += ` ORDER BY p.percentage_discount DESC`
          break
        default: // relevance
          sql += ` ORDER BY p.relevance_score DESC, p.review_rating DESC, p.total_reviews DESC`
      }
      
      // Paginación
      const offset = (page - 1) * limit
      sql += ` LIMIT ? OFFSET ?`
      params.push(Number(limit), Number(offset))
      
      console.log('🔍 Final SQL:', sql)
      console.log('📊 Final Params:', params)
      
      // Ejecutar query principal
      const products = await executeQuery(sql, params)
      
      // Query para conteo total (sin LIMIT/OFFSET)
      let countSql = `
        SELECT COUNT(*) as total
        FROM products p
        WHERE p.status = 1 
          AND p.visible = 1 
          AND p.store_authorized = 1
          AND p.stock > 0
      `
      const countParams = []
      
      // Aplicar los mismos filtros para el conteo
      if (query && query.trim()) {
        countSql += ` AND (p.name LIKE ? OR p.brand LIKE ? OR p.description LIKE ?)`
        const likeQuery = `%${query.trim()}%`
        countParams.push(likeQuery, likeQuery, likeQuery)
      }
      
      if (filters.brand) {
        countSql += ` AND p.brand = ?`
        countParams.push(filters.brand)
      }
      
      if (filters.category_id) {
        countSql += ` AND p.category_id = ?`
        countParams.push(Number(filters.category_id))
      }
      
      if (filters.min_price) {
        countSql += ` AND p.sales_price >= ?`
        countParams.push(Number(filters.min_price))
      }
      
      if (filters.max_price) {
        countSql += ` AND p.sales_price <= ?`
        countParams.push(Number(filters.max_price))
      }
      
      if (filters.free_shipping === 'true') {
        countSql += ` AND p.has_free_shipping = 1`
      }
      
      if (filters.fulfillment_type) {
        countSql += ` AND p.fulfillment_type = ?`
        countParams.push(filters.fulfillment_type)
      }
      
      if (filters.min_rating) {
        countSql += ` AND p.review_rating >= ?`
        countParams.push(Number(filters.min_rating))
      }
      
      const [countResult] = await executeQuery(countSql, countParams)
      
      return {
        products: products.map(product => ({
          id: product.id,
          name: product.name,
          description: product.description,
          brand: product.brand,
          price: parseFloat(product.sales_price),
          originalPrice: parseFloat(product.list_price),
          stock: product.stock,
          category: product.category_name,
          store: product.store_name,
          rating: parseFloat(product.review_rating),
          reviewCount: product.total_reviews,
          image: product.main_image,
          freeShipping: product.has_free_shipping === 1,
          fulfillment: product.fulfillment_type,
          discount: product.percentage_discount,
          relevance: parseFloat(product.relevance_score)
        })),
        total: countResult.total,
        page: Number(page),
        limit: Number(limit)
      }
      
    } catch (error) {
      console.error('Search error:', error)
      throw error
    }
  }

  // Función para debugging directo
  static async debugSearch() {
    try {
      // Query SIN parámetros primero
      const sql1 = `
        SELECT 
          p.id,
          p.name,
          p.brand
        FROM products p
        WHERE p.status = 1 
        LIMIT 5
      `
      
      console.log('🔍 Testing query without params...')
      const result1 = await executeQuery(sql1, [])
      console.log('✅ Query without params works:', result1.length, 'results')
      
      // Query CON parámetros simples
      const sql2 = `
        SELECT 
          p.id,
          p.name,
          p.brand
        FROM products p
        WHERE p.status = ?
        LIMIT ?
      `
      
      console.log('🔍 Testing query with simple params...')
      const result2 = await executeQuery(sql2, [1, 5])
      console.log('✅ Query with simple params works:', result2.length, 'results')
      
      // Query con LIKE
      const sql3 = `
        SELECT 
          p.id,
          p.name,
          p.brand
        FROM products p
        WHERE p.name LIKE ?
        LIMIT ?
      `
      
      console.log('🔍 Testing query with LIKE...')
      const result3 = await executeQuery(sql3, ['%phone%', 5])
      console.log('✅ Query with LIKE works:', result3.length, 'results')
      
      return result3
      
    } catch (error) {
      console.error('❌ Debug search error:', error)
      throw error
    }
  }
}