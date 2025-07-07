// src/services/searchService.js - Fixed version
import { executeQuery } from '../config/database.js'
import { FacetService } from './facetService.js'; // Add this import

export class SearchService {
  /**
   * Búsqueda principal de productos con facetas integradas
   */
  static async searchProductsWithFacets (params) {
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
  static async searchProducts (params) {
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
        facets ? FacetService.getFacets(query, filters) : Promise.resolve(null) // FIXED: Use FacetService instead of this.getFacets
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
        query,
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
  static buildSearchQuery (query, filters, sortBy, sortOrder, limit, offset) {
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
    sql += ' LIMIT ? OFFSET ?'
    queryParams.push(limit, offset)

    return { sql, countSql, queryParams }
  }

  /**
   * Obtener facetas para búsqueda - FIXED METHOD
   */
  static async getFacetsForSearch (query, filters, mode) {
    try {
      if (mode === 'cached' && !query && Object.keys(filters).length === 0) {
        // Usar FacetService para facetas rápidas
        return await FacetService.getQuickFacets(filters.category_id)
      } else {
        // Usar FacetService para facetas dinámicas
        return await FacetService.getFacets(query, filters, filters.category_id)
      }
    } catch (error) {
      console.error('Error getting facets for search:', error)
      return null
    }
  }

  /**
   * Determinar estrategia de búsqueda basada en el query
   */
  static determineSearchStrategy (query) {
    // Implementar lógica para determinar si es búsqueda exacta, fuzzy, etc.
    if (query.includes('"')) return 'exact'
    if (query.length < 3) return 'prefix'
    return 'fulltext'
  }

  /**
   * Construir condición de búsqueda de texto
   */
  static buildTextSearchCondition (query, strategy) {
    const searchTerms = query.trim().split(/\s+/)

    switch (strategy) {
      case 'exact':
        const exactQuery = query.replace(/"/g, '')
        return {
          condition: '(p.name LIKE ? OR p.brand LIKE ? OR p.description LIKE ?)',
          params: [`%${exactQuery}%`, `%${exactQuery}%`, `%${exactQuery}%`]
        }

      case 'prefix':
        return {
          condition: '(p.name LIKE ? OR p.brand LIKE ?)',
          params: [`${query}%`, `${query}%`]
        }

      default: // fulltext
        const conditions = searchTerms.map(() => '(p.name LIKE ? OR p.brand LIKE ? OR p.description LIKE ?)').join(' AND ')
        const params = searchTerms.flatMap(term => [`%${term}%`, `%${term}%`, `%${term}%`])
        return { condition: conditions, params }
    }
  }

  /**
   * Construir condiciones de filtros
   */
  static buildFilterConditions (filters, queryParams) {
    const conditions = []

    // Filtro por categoría (con diferentes niveles)
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

    // Filtro por rango de precios
    if (filters.min_price) {
      conditions.push('p.sales_price >= ?')
      queryParams.push(parseFloat(filters.min_price))
    }

    if (filters.max_price) {
      conditions.push('p.sales_price <= ?')
      queryParams.push(parseFloat(filters.max_price))
    }

    // Filtro por marca
    if (filters.brand) {
      if (Array.isArray(filters.brand)) {
        const placeholders = filters.brand.map(() => '?').join(',')
        conditions.push(`p.brand IN (${placeholders})`)
        queryParams.push(...filters.brand)
      } else {
        conditions.push('p.brand = ?')
        queryParams.push(filters.brand)
      }
    }

    // Filtro por tienda
    if (filters.store_id) {
      if (Array.isArray(filters.store_id)) {
        const placeholders = filters.store_id.map(() => '?').join(',')
        conditions.push(`p.store_id IN (${placeholders})`)
        queryParams.push(...filters.store_id)
      } else {
        conditions.push('p.store_id = ?')
        queryParams.push(filters.store_id)
      }
    }

    // Filtro por rating mínimo
    if (filters.min_rating) {
      conditions.push('p.review_rating >= ?')
      queryParams.push(parseFloat(filters.min_rating))
    }

    // Filtro por envío gratis
    if (filters.free_shipping === 'true' || filters.free_shipping === true) {
      conditions.push('p.has_free_shipping = 1')
    }

    // Filtro por productos digitales
    if (filters.digital !== undefined) {
      conditions.push('p.digital = ?')
      queryParams.push(filters.digital === 'true' || filters.digital === true ? 1 : 0)
    }

    return conditions
  }

  /**
   * Construir cláusula ORDER BY
   */
  static buildOrderBy (sortBy, sortOrder, query) {
    const direction = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC'

    switch (sortBy) {
      case 'price':
        return ` ORDER BY p.sales_price ${direction}`
      case 'rating':
        return ` ORDER BY p.review_rating ${direction}, p.total_reviews DESC`
      case 'newest':
        return ` ORDER BY p.created_at ${direction}`
      case 'name':
        return ` ORDER BY p.name ${direction}`
      case 'discount': // 🆕 NUEVO SORTING POR DESCUENTO
        return ` ORDER BY p.percentage_discount ${direction}, p.sales_price ASC`
      case 'reviews': // 🆕 SORTING POR CANTIDAD DE REVIEWS
        return ` ORDER BY p.total_reviews ${direction}, p.review_rating DESC`
      case 'relevance':
      default:
        // Si hay query de búsqueda, ordenar por relevancia primero
        if (query && query.trim()) {
          return ` ORDER BY p.relevance_score ${direction}, p.sales_price ASC`
        }
        return ` ORDER BY p.relevance_score ${direction}, p.sales_price ASC`
    }
  }

  /**
   * Obtener conteo de resultados
   */
  static async getSearchCount (countSql, params) {
    try {
      const result = await executeQuery(countSql, params)
      return result
    } catch (error) {
      console.error('Error getting search count:', error)
      return [{ total: 0 }]
    }
  }

  /**
   * Enriquecer productos con datos adicionales
   */
  static async enrichProducts (products) {
    if (!products || products.length === 0) return []

    try {
      const productIds = products.map(p => p.id)

      // Obtener imágenes, variaciones y atributos en paralelo
      const [images, variations, attributes] = await Promise.all([
        this.getProductImages(productIds),
        this.getProductVariations(productIds),
        this.getProductAttributes(productIds)
      ])

      // Crear mapas para acceso rápido
      const imagesMap = this.groupBy(images, 'product_id')
      const variationsMap = this.groupBy(variations, 'product_id')
      const attributesMap = this.groupBy(attributes, 'product_id')

      // Enriquecer cada producto
      return products.map(product => ({
        ...product,
        images: imagesMap[product.id] || [],
        variations: variationsMap[product.id] || [],
        attributes: attributesMap[product.id] || [],
        // Campos calculados
        hasDiscount: product.percentage_discount > 0,
        finalPrice: product.sales_price,
        originalPrice: product.list_price,
        isInStock: product.stock > 0,
        isPopular: product.total_reviews > 50 && product.review_rating >= 4.0
      }))
    } catch (error) {
      console.error('Error enriching products:', error)
      return products
    }
  }

  /**
   * Obtener imágenes de productos
   */
  static async getProductImages (productIds) {
    if (!productIds.length) return []

    const placeholders = productIds.map(() => '?').join(',')
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

    return await executeQuery(sql, productIds)
  }

  /**
   * Obtener variaciones de productos
   */
  static async getProductVariations (productIds) {
    if (!productIds.length) return []

    const placeholders = productIds.map(() => '?').join(',')
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

    return await executeQuery(sql, productIds)
  }

  /**
   * Obtener atributos de productos
   */
  static async getProductAttributes (productIds) {
    if (!productIds.length) return []

    const placeholders = productIds.map(() => '?').join(',')
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
   * Normalizar filtros
   */
  static normalizeFilters (filters) {
    const normalized = {}

    Object.keys(filters).forEach(key => {
      if (filters[key] !== undefined && filters[key] !== null && filters[key] !== '') {
        normalized[key] = filters[key]
      }
    })

    return normalized
  }

  /**
   * Agrupar array por clave
   */
  static groupBy (array, key) {
    return array.reduce((groups, item) => {
      const group = groups[item[key]] || []
      group.push(item)
      groups[item[key]] = group
      return groups
    }, {})
  }

  /**
   * Búsqueda simple (mantener compatibilidad)
   */
  static async simpleSearch (query = '', page = 1, limit = 20, filters = {}) {
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
  static async getAutocompleteSuggestions (query, limit = 10) {
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

      const suggestions = await executeQuery(sql, [
        likeQuery, Math.ceil(limit / 3),
        likeQuery, Math.ceil(limit / 3),
        likeQuery, Math.ceil(limit / 3),
        limit
      ])

      return suggestions
    } catch (error) {
      console.error('Autocomplete error:', error)
      return []
    }
  }
}
