// src/services/searchService.js - Versión completa con facetas integradas
import { executeQuery } from '../config/database.js'

export class SearchService {
  
  /**
   * Búsqueda principal de productos con facetas integradas
   */
  static async searchProductsWithFacets(params) {
    const {
      query = '',
      page = 1,
      limit = 20,
      sortBy = 'relevance',
      sortOrder = 'desc',
      filters = {},
      includeFacets = true,
      facetMode = 'dynamic' // 'dynamic' o 'cached'
    } = params

    const startTime = Date.now()

    try {
      // Ejecutar búsqueda de productos y facetas en paralelo
      const [searchResults, facets] = await Promise.all([
        this.searchProducts({
          query,
          page,
          limit,
          sortBy,
          sortOrder,
          filters,
          facets: false // No incluir facetas en la búsqueda principal
        }),
        includeFacets ? this.getFacetsForSearch(query, filters, facetMode) : null
      ])

      return {
        ...searchResults,
        facets: facets || null,
        facetMode: includeFacets ? facetMode : null,
        meta: {
          ...searchResults.meta,
          facetsIncluded: includeFacets,
          executionTime: Date.now() - startTime
        }
      }
    } catch (error) {
      console.error('Search with facets error:', error)
      throw error
    }
  }

  /**
   * Búsqueda principal de productos (método actualizado)
   */
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
        facets ? this.getFacets(query, filters) : Promise.resolve(null)
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
        filters: this.normalizeFilters(filters),
        meta: {
          executionTime: Date.now(),
          timestamp: new Date().toISOString()
        }
      }
    } catch (error) {
      console.error('Search error:', error)
      throw new Error(`Search failed: ${error.message}`)
    }
  }

  /**
   * Construye la consulta SQL de búsqueda
   */
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
        p.relevance_score,
        p.created_at,
        p.updated_at
      FROM products p
      WHERE p.status = 1 
        AND p.visible = 1 
        AND p.store_authorized = 1
        AND p.stock > 0
    `
  
    const queryParams = []
    const whereConditions = []
  
    // Búsqueda por texto con diferentes estrategias
    if (query && query.trim()) {
      const searchStrategy = this.determineSearchStrategy(query)
      const searchCondition = this.buildTextSearchCondition(query, searchStrategy)
      whereConditions.push(searchCondition.condition)
      queryParams.push(...searchCondition.params)
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
  
    console.log('🔍 SQL:', sql.trim())
    console.log('📊 Params:', queryParams)
  
    return { sql, countSql, queryParams }
  }

  /**
   * Determina la estrategia de búsqueda basada en el query
   */
  static determineSearchStrategy(query) {
    const trimmedQuery = query.trim()
    
    // Búsqueda exacta si está entre comillas
    if (trimmedQuery.startsWith('"') && trimmedQuery.endsWith('"')) {
      return 'exact'
    }
    
    // Búsqueda por SKU si parece un código de producto
    if (/^[A-Z0-9\-_]{6,}$/i.test(trimmedQuery)) {
      return 'sku'
    }
    
    // Búsqueda por marca si es una sola palabra conocida
    if (trimmedQuery.split(' ').length === 1 && trimmedQuery.length > 2) {
      return 'brand_first'
    }
    
    // Búsqueda compleja si tiene múltiples palabras
    if (trimmedQuery.split(' ').length > 3) {
      return 'complex'
    }
    
    return 'standard'
  }

  /**
   * Construye la condición de búsqueda de texto
   */
  static buildTextSearchCondition(query, strategy) {
    const trimmedQuery = query.trim()
    
    switch (strategy) {
      case 'exact':
        const exactQuery = trimmedQuery.slice(1, -1) // Remover comillas
        return {
          condition: `(p.name LIKE ? OR p.description LIKE ?)`,
          params: [`%${exactQuery}%`, `%${exactQuery}%`]
        }
        
      case 'sku':
        return {
          condition: `(p.sku = ? OR p.sku LIKE ?)`,
          params: [trimmedQuery, `%${trimmedQuery}%`]
        }
        
      case 'brand_first':
        const likeQuery = `%${trimmedQuery}%`
        return {
          condition: `(
            p.brand LIKE ? OR 
            p.name LIKE ? OR 
            p.search_text LIKE ?
          )`,
          params: [likeQuery, likeQuery, likeQuery]
        }
        
      case 'complex':
        // Búsqueda por palabras individuales
        const words = trimmedQuery.split(' ').filter(word => word.length > 2)
        const wordConditions = words.map(() => 
          `(p.name LIKE ? OR p.brand LIKE ? OR p.search_text LIKE ?)`
        ).join(' AND ')
        const wordParams = words.flatMap(word => {
          const wordQuery = `%${word}%`
          return [wordQuery, wordQuery, wordQuery]
        })
        
        return {
          condition: `(${wordConditions})`,
          params: wordParams
        }
        
      default: // standard
        const standardQuery = `%${trimmedQuery}%`
        return {
          condition: `(
            p.name LIKE ? OR 
            p.brand LIKE ? OR 
            p.search_text LIKE ?
          )`,
          params: [standardQuery, standardQuery, standardQuery]
        }
    }
  }

  /**
   * Construye condiciones de filtros
   */
  static buildFilterConditions(filters, queryParams) {
    const conditions = []

    // Filtro por categoría
    if (filters.category_id) {
      if (Array.isArray(filters.category_id)) {
        conditions.push(`p.category_id IN (${filters.category_id.map(() => '?').join(',')})`)
        queryParams.push(...filters.category_id.map(id => parseInt(id)))
      } else {
        conditions.push('p.category_id = ?')
        queryParams.push(parseInt(filters.category_id))
      }
    }

    // Filtros de categoría por nivel
    if (filters.category_lvl0) {
      conditions.push('p.category_lvl0 = ?')
      queryParams.push(filters.category_lvl0)
    }

    if (filters.category_lvl1) {
      conditions.push('p.category_lvl1 = ?')
      queryParams.push(filters.category_lvl1)
    }

    if (filters.category_lvl2) {
      conditions.push('p.category_lvl2 = ?')
      queryParams.push(filters.category_lvl2)
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
        queryParams.push(...filters.store_id.map(id => parseInt(id)))
      } else {
        conditions.push('p.store_id = ?')
        queryParams.push(parseInt(filters.store_id))
      }
    }

    // Filtro por rango de precio
    if (filters.min_price) {
      conditions.push('p.sales_price >= ?')
      queryParams.push(parseFloat(filters.min_price))
    }

    if (filters.max_price) {
      conditions.push('p.sales_price <= ?')
      queryParams.push(parseFloat(filters.max_price))
    }

    // Filtro por rating mínimo
    if (filters.min_rating) {
      conditions.push('p.review_rating >= ?')
      queryParams.push(parseFloat(filters.min_rating))
    }

    // Filtro por envío gratis
    if (filters.free_shipping === true || filters.free_shipping === 'true') {
      conditions.push('p.has_free_shipping = 1')
    }

    // Filtro por fulfillment
    if (filters.fulfillment_type) {
      if (Array.isArray(filters.fulfillment_type)) {
        conditions.push(`p.fulfillment_type IN (${filters.fulfillment_type.map(() => '?').join(',')})`)
        queryParams.push(...filters.fulfillment_type)
      } else {
        conditions.push('p.fulfillment_type = ?')
        queryParams.push(filters.fulfillment_type)
      }
    }

    // Filtro por productos digitales
    if (filters.digital === true || filters.digital === 'true') {
      conditions.push('p.digital = 1')
    }

    // Filtro por productos con descuento
    if (filters.has_discount === true || filters.has_discount === 'true') {
      conditions.push('p.percentage_discount > 0')
    }

    // Filtro por productos de alto valor
    if (filters.big_ticket === true || filters.big_ticket === 'true') {
      conditions.push('p.big_ticket = 1')
    }

    // Filtro por envío express
    if (filters.super_express === true || filters.super_express === 'true') {
      conditions.push('p.super_express = 1')
    }

    // Filtro por recoger en tienda
    if (filters.is_store_pickup === true || filters.is_store_pickup === 'true') {
      conditions.push('p.is_store_pickup = 1')
    }

    // Filtro por productos disponibles por pedido
    if (filters.back_order === true || filters.back_order === 'true') {
      conditions.push('p.back_order = 1')
    }

    // Filtro por rango de días de envío
    if (filters.max_shipping_days) {
      conditions.push('p.shipping_days <= ?')
      queryParams.push(parseInt(filters.max_shipping_days))
    }

    // Filtro por stock mínimo
    if (filters.min_stock) {
      conditions.push('p.stock >= ?')
      queryParams.push(parseInt(filters.min_stock))
    }

    // Filtro por fecha de creación
    if (filters.created_after) {
      conditions.push('p.created_at >= ?')
      queryParams.push(filters.created_after)
    }

    if (filters.created_before) {
      conditions.push('p.created_at <= ?')
      queryParams.push(filters.created_before)
    }

    return conditions
  }

  /**
   * Construir ORDER BY
   */
  static buildOrderBy(sortBy, sortOrder, query) {
    const order = sortOrder.toLowerCase() === 'asc' ? 'ASC' : 'DESC'
    
    switch (sortBy) {
      case 'price':
        return ` ORDER BY p.sales_price ${order}`
      case 'price_high':
        return ` ORDER BY p.sales_price DESC`
      case 'price_low':
        return ` ORDER BY p.sales_price ASC`
      case 'rating':
        return ` ORDER BY p.review_rating ${order}, p.total_reviews DESC`
      case 'reviews':
        return ` ORDER BY p.total_reviews ${order}, p.review_rating DESC`
      case 'newest':
        return ` ORDER BY p.created_at DESC`
      case 'oldest':
        return ` ORDER BY p.created_at ASC`
      case 'name':
        return ` ORDER BY p.name ${order}`
      case 'brand':
        return ` ORDER BY p.brand ${order}, p.name ASC`
      case 'discount':
        return ` ORDER BY p.percentage_discount ${order}, p.sales_price ASC`
      case 'shipping':
        return ` ORDER BY p.has_free_shipping DESC, p.shipping_cost ASC, p.shipping_days ASC`
      case 'stock':
        return ` ORDER BY p.stock ${order}`
      case 'relevance':
      default:
        if (query && query.trim()) {
          return ` ORDER BY p.relevance_score DESC, p.review_rating DESC, p.total_reviews DESC`
        }
        return ` ORDER BY p.relevance_score DESC, p.sales_price ASC`
    }
  }

  /**
   * Obtener conteo total
   */
  static async getSearchCount(countSql, queryParams) {
    return await executeQuery(countSql, queryParams)
  }

  /**
   * Enriquecer productos con datos adicionales
   */
  static async enrichProducts(products) {
    if (!products.length) return products

    const productIds = products.map(p => p.id)
    const placeholders = productIds.map(() => '?').join(',')

    // Obtener imágenes adicionales, variaciones y atributos en paralelo
    const [images, variations, attributes] = await Promise.all([
      this.getProductImages(productIds, placeholders),
      this.getProductVariations(productIds, placeholders),
      this.getProductAttributes(productIds, placeholders)
    ])

    // Mapear datos adicionales
    const imagesMap = this.groupByProductId(images)
    const variationsMap = this.groupByProductId(variations, true)
    const attributesMap = this.groupByProductId(attributes)

    // Enriquecer productos
    return products.map(product => ({
      ...this.normalizeProduct(product),
      images: imagesMap[product.id] || [],
      variations: variationsMap[product.id] || { count: 0, totalStock: 0, options: [] },
      attributes: attributesMap[product.id] || [],
      // Campos calculados
      hasDiscount: product.percentage_discount > 0,
      finalPrice: parseFloat(product.sales_price),
      originalPrice: parseFloat(product.list_price),
      savings: product.list_price ? parseFloat(product.list_price) - parseFloat(product.sales_price) : 0,
      savingsPercentage: product.list_price ? 
        Math.round(((parseFloat(product.list_price) - parseFloat(product.sales_price)) / parseFloat(product.list_price)) * 100) : 0,
      freeShipping: product.has_free_shipping === 1,
      inStock: product.stock > 0,
      lowStock: product.stock <= 5,
      isNew: this.isNewProduct(product.created_at),
      isPremium: parseFloat(product.sales_price) > 1000,
      shippingInfo: this.getShippingInfo(product),
      url: `/products/${product.id}`,
      shareUrl: `${process.env.FRONTEND_URL || 'https://example.com'}/products/${product.id}`
    }))
  }

  /**
   * Obtener imágenes de productos
   */
  static async getProductImages(productIds, placeholders) {
    const sql = `
      SELECT 
        product_id, 
        image_url, 
        thumbnail_url, 
        image_order
      FROM product_images 
      WHERE product_id IN (${placeholders})
      ORDER BY product_id, image_order
    `
    
    const results = await executeQuery(sql, productIds)
    return results.map(img => ({
      ...img,
      url: img.image_url,
      thumbnail: img.thumbnail_url,
      order: img.image_order
    }))
  }

  /**
   * Obtener variaciones de productos
   */
  static async getProductVariations(productIds, placeholders) {
    const sql = `
      SELECT 
        product_id,
        COUNT(*) as variation_count, 
        SUM(stock) as total_stock,
        GROUP_CONCAT(
          JSON_OBJECT(
            'sku', sku,
            'size', size_name,
            'color', color_name,
            'stock', stock,
            'priceModifier', price_modifier
          )
        ) as options
      FROM product_variations 
      WHERE product_id IN (${placeholders})
      GROUP BY product_id
    `
    
    const results = await executeQuery(sql, productIds)
    return results.map(variation => ({
      product_id: variation.product_id,
      count: variation.variation_count,
      totalStock: variation.total_stock || 0,
      options: variation.options ? JSON.parse(`[${variation.options}]`) : []
    }))
  }

  /**
   * Obtener atributos de productos
   */
  static async getProductAttributes(productIds, placeholders) {
    const sql = `
      SELECT 
        product_id,
        attribute_name as name,
        attribute_value as value
      FROM product_attributes 
      WHERE product_id IN (${placeholders})
      ORDER BY product_id, attribute_name
    `
    
    return await executeQuery(sql, productIds)
  }

  /**
   * Agrupar resultados por product_id
   */
  static groupByProductId(items, isVariation = false) {
    const grouped = {}
    
    items.forEach(item => {
      if (!grouped[item.product_id]) {
        grouped[item.product_id] = isVariation ? item : []
      }
      
      if (!isVariation) {
        grouped[item.product_id].push(item)
      }
    })
    
    return grouped
  }

  /**
   * Normalizar datos del producto
   */
  static normalizeProduct(product) {
    return {
      id: product.id,
      name: product.name,
      description: product.description,
      shortDescription: product.short_description,
      sku: product.sku,
      brand: product.brand,
      price: parseFloat(product.sales_price),
      listPrice: parseFloat(product.list_price),
      shippingCost: parseFloat(product.shipping_cost),
      discount: product.percentage_discount,
      stock: product.stock,
      categoryId: product.category_id,
      categoryName: product.category_name,
      categoryPath: product.category_path,
      storeId: product.store_id,
      storeName: product.store_name,
      storeLogo: product.store_logo,
      storeRating: parseFloat(product.store_rating),
      isDigital: product.digital === 1,
      isBigTicket: product.big_ticket === 1,
      allowBackOrder: product.back_order === 1,
      allowStorePickup: product.is_store_pickup === 1,
      hasSuperExpress: product.super_express === 1,
      shippingDays: product.shipping_days,
      rating: parseFloat(product.review_rating) || 0,
      reviewCount: product.total_reviews || 0,
      mainImage: product.main_image,
      thumbnail: product.thumbnail,
      fulfillmentType: product.fulfillment_type,
      freeShipping: product.has_free_shipping === 1,
      relevanceScore: parseFloat(product.relevance_score),
      createdAt: product.created_at,
      updatedAt: product.updated_at
    }
  }

  /**
   * Verificar si es un producto nuevo (últimos 30 días)
   */
  static isNewProduct(createdAt) {
    if (!createdAt) return false
    const thirtyDaysAgo = new Date()
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30)
    return new Date(createdAt) > thirtyDaysAgo
  }

  /**
   * Obtener información de envío
   */
  static getShippingInfo(product) {
    return {
      isFree: product.has_free_shipping === 1,
      cost: parseFloat(product.shipping_cost),
      days: product.shipping_days,
      hasExpress: product.super_express === 1,
      allowPickup: product.is_store_pickup === 1,
      fulfillmentType: product.fulfillment_type
    }
  }

  /**
   * Obtener facetas para búsqueda
   */
  static async getFacetsForSearch(query, filters, mode) {
    try {
      if (mode === 'cached' && !query && Object.keys(filters).length === 0) {
        // Usar FacetService para facetas rápidas
        const { FacetService } = await import('./facetService.js')
        return await FacetService.getQuickFacets(filters.category_id)
      } else {
        // Usar FacetService para facetas dinámicas
        const { FacetService } = await import('./facetService.js')
        return await FacetService.getFacets(query, filters, filters.category_id)
      }
    } catch (error) {
      console.error('Error getting facets for search:', error)
      return null
    }
  }

  /**
   * Normalizar filtros
   */
  static normalizeFilters(filters) {
    const normalized = {}
    
    Object.keys(filters).forEach(key => {
      if (filters[key] !== undefined && filters[key] !== null && filters[key] !== '') {
        normalized[key] = filters[key]
      }
    })
    
    return normalized
  }

  // Mantener métodos existentes como simpleSearch, getAutocompleteSuggestions, etc.
  
  /**
   * Búsqueda simple (mantener compatibilidad)
   */
  static async simpleSearch(query = '', page = 1, limit = 20, filters = {}) {
    try {
      const results = await this.searchProducts({
        query,
        page,
        limit,
        filters,
        facets: false
      })
      
      return {
        products: results.products,
        total: results.pagination.total,
        page: results.pagination.page,
        limit: results.pagination.limit
      }
    } catch (error) {
      console.error('Simple search error:', error)
      throw error
    }
  }

  /**
   * Autocompletado de búsquedas
   */
  static async getAutocompleteSuggestions(query, limit = 10) {
    if (!query || query.length < 2) return []

    try {
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
    } catch (error) {
      console.error('Autocomplete error:', error)
      return []
    }
  }

  /**
   * Obtener producto por ID
   */
  static async getProductById(productId) {
    try {
      const sql = `
        SELECT * FROM products 
        WHERE id = ? AND status = 1 AND visible = 1 AND store_authorized = 1
      `
      
      const [product] = await executeQuery(sql, [productId])
      
      if (!product) return null
      
      // Enriquecer con datos adicionales
      const enriched = await this.enrichProducts([product])
      return enriched[0] || null
    } catch (error) {
      console.error('Get product by ID error:', error)
      return null
    }
  }

  /**
   * Productos trending/populares
   */
  static async getTrendingProducts(limit = 20, categoryId = null) {
    try {
      let sql = `
        SELECT 
          p.*,
          (p.total_reviews * p.review_rating) as popularity_score
        FROM products p
        WHERE p.status = 1 AND p.visible = 1 AND p.store_authorized = 1
          AND p.stock > 0
          AND p.total_reviews > 0
      `
      const params = []
      
      if (categoryId) {
        sql += ` AND p.category_id = ?`
        params.push(categoryId)
      }
      
      sql += `
        ORDER BY popularity_score DESC, p.created_at DESC
        LIMIT ?
      `
      params.push(limit)
      
      const products = await executeQuery(sql, params)
      return await this.enrichProducts(products)
    } catch (error) {
      console.error('Get trending products error:', error)
      return []
    }
  }

  /**
   * Debugging directo
   */
  static async debugSearch() {
    try {
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