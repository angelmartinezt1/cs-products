// src/services/facetService.js
import { executeQuery } from '../config/database.js'

export class FacetService {
  
  /**
   * Obtiene todas las facetas disponibles con conteos dinámicos
   */
  static async getFacets(searchQuery = '', filters = {}, categoryId = null) {
    try {
      const whereClause = this.buildWhereClause(searchQuery, filters, categoryId)
      const params = this.buildQueryParams(searchQuery, filters, categoryId)
      
      // Ejecutar todas las consultas de facetas en paralelo
      const [
        brands,
        categories,
        priceRanges,
        stores,
        fulfillmentTypes,
        ratings,
        features,
        shippingOptions
      ] = await Promise.all([
        this.getBrandFacets(whereClause, params, filters),
        this.getCategoryFacets(whereClause, params, filters, categoryId),
        this.getPriceRangeFacets(whereClause, params, filters),
        this.getStoreFacets(whereClause, params, filters),
        this.getFulfillmentFacets(whereClause, params, filters),
        this.getRatingFacets(whereClause, params, filters),
        this.getFeatureFacets(whereClause, params, filters),
        this.getShippingFacets(whereClause, params, filters)
      ])

      return {
        brands,
        categories,
        priceRanges,
        stores,
        fulfillmentTypes,
        ratings,
        features,
        shippingOptions,
        appliedFilters: this.getAppliedFilters(filters),
        totalProducts: await this.getTotalProductCount(whereClause, params)
      }
    } catch (error) {
      console.error('Error getting facets:', error)
      throw error
    }
  }

  /**
   * Construye la cláusula WHERE base para facetas
   */
  static buildWhereClause(searchQuery, filters, categoryId) {
    let whereClause = `
      WHERE p.status = 1 
        AND p.visible = 1 
        AND p.store_authorized = 1
        AND p.stock > 0
    `

    // Búsqueda por texto
    if (searchQuery && searchQuery.trim()) {
      whereClause += ` AND (
        p.name LIKE ? OR 
        p.brand LIKE ? OR 
        p.search_text LIKE ?
      )`
    }

    // Filtro por categoría específica
    if (categoryId) {
      whereClause += ` AND p.category_id = ?`
    }

    // Aplicar otros filtros
    const filterConditions = this.buildFilterConditions(filters)
    if (filterConditions.length > 0) {
      whereClause += ` AND (${filterConditions.join(' AND ')})`
    }

    return whereClause
  }

  /**
   * Construye los parámetros para las consultas
   */
  static buildQueryParams(searchQuery, filters, categoryId) {
    const params = []

    // Parámetros de búsqueda por texto
    if (searchQuery && searchQuery.trim()) {
      const likeQuery = `%${searchQuery.trim()}%`
      params.push(likeQuery, likeQuery, likeQuery)
    }

    // Parámetro de categoría
    if (categoryId) {
      params.push(categoryId)
    }

    // Parámetros de filtros
    const filterParams = this.getFilterParams(filters)
    params.push(...filterParams)

    return params
  }

  /**
   * Facetas de marcas
   */
  static async getBrandFacets(whereClause, params, filters) {
    if (filters.brand) return [] // No mostrar si ya está filtrado

    const sql = `
      SELECT 
        p.brand as value, 
        COUNT(*) as count,
        AVG(p.review_rating) as avg_rating,
        MIN(p.sales_price) as min_price,
        MAX(p.sales_price) as max_price
      FROM products p
      ${whereClause}
      AND p.brand IS NOT NULL
      GROUP BY p.brand
      HAVING count >= 2
      ORDER BY count DESC, avg_rating DESC
      LIMIT 30
    `
    
    const results = await executeQuery(sql, params)
    return results.map(row => ({
      value: row.value,
      count: row.count,
      avgRating: parseFloat(row.avg_rating),
      priceRange: {
        min: parseFloat(row.min_price),
        max: parseFloat(row.max_price)
      }
    }))
  }

/**
 * Facetas de categorías jerárquicas (CORREGIDO)
 */
static async getCategoryFacets(whereClause, params, filters, currentCategoryId) {
  // Si no estamos en una categoría específica, mostrar nivel 0
  if (!currentCategoryId) {
    const sql = `
      SELECT 
        p.category_lvl0 as value,
        p.category_id,
        COUNT(*) as count,
        AVG(p.sales_price) as avg_price
      FROM products p
      ${whereClause}
      AND p.category_lvl0 IS NOT NULL
      GROUP BY p.category_lvl0, p.category_id
      ORDER BY count DESC
      LIMIT 15
    `
    return await executeQuery(sql, params)
  } else {
    // Obtener subcategorías del nivel actual
    const subCategorySql = `
      SELECT 
        p.category_lvl2 as value,
        COUNT(*) as count
      FROM products p
      ${whereClause}
      AND p.category_lvl2 IS NOT NULL
      AND p.category_lvl2 != p.category_lvl1
      GROUP BY p.category_lvl2
      ORDER BY count DESC
      LIMIT 20
    `
    return await executeQuery(subCategorySql, params)
  }
}

// Y actualizar el método principal getFacets para manejar mejor las categorías:

static async getFacets(searchQuery = '', filters = {}, categoryId = null) {
  try {
    const whereClause = this.buildWhereClause(searchQuery, filters, categoryId)
    const params = this.buildQueryParams(searchQuery, filters, categoryId)
    
    // Ejecutar todas las consultas de facetas en paralelo
    const [
      brands,
      categories,
      priceRanges,
      stores,
      fulfillmentTypes,
      ratings,
      features,
      shippingOptions
    ] = await Promise.all([
      this.getBrandFacets(whereClause, params, filters),
      this.getCategoryFacets(whereClause, params, filters, categoryId),
      this.getPriceRangeFacets(whereClause, params, filters),
      this.getStoreFacets(whereClause, params, filters),
      this.getFulfillmentFacets(whereClause, params, filters),
      this.getRatingFacets(whereClause, params, filters),
      this.getFeatureFacets(whereClause, params, filters),
      this.getShippingFacets(whereClause, params, filters)
    ])

    return {
      brands: Array.isArray(brands) ? brands : [],
      categories: Array.isArray(categories) ? categories : [],
      priceRanges: Array.isArray(priceRanges) ? priceRanges : [],
      stores: Array.isArray(stores) ? stores : [],
      fulfillmentTypes: Array.isArray(fulfillmentTypes) ? fulfillmentTypes : [],
      ratings: Array.isArray(ratings) ? ratings : [],
      features: Array.isArray(features) ? features : [],
      shippingOptions: Array.isArray(shippingOptions) ? shippingOptions : [],
      appliedFilters: this.getAppliedFilters(filters),
      totalProducts: await this.getTotalProductCount(whereClause, params)
    }
  } catch (error) {
    console.error('Error getting facets:', error)
    throw error
  }
}

  /**
   * Facetas de rangos de precio dinámicos
   */
  static async getPriceRangeFacets(whereClause, params, filters) {
    if (filters.min_price || filters.max_price) return []

    // Primero obtener estadísticas de precio
    const statsSql = `
      SELECT 
        MIN(p.sales_price) as min_price,
        MAX(p.sales_price) as max_price,
        AVG(p.sales_price) as avg_price,
        COUNT(*) as total_count
      FROM products p
      ${whereClause}
      AND p.sales_price > 0
    `
    
    const [stats] = await executeQuery(statsSql, params)
    if (!stats || stats.total_count === 0) return []

    // Generar rangos dinámicos
    const ranges = this.generatePriceRanges(stats.min_price, stats.max_price, stats.avg_price)
    
    // Obtener conteos para cada rango
    const rangePromises = ranges.map(async (range) => {
      const rangeSql = `
        SELECT COUNT(*) as count
        FROM products p
        ${whereClause}
        AND p.sales_price >= ?
        ${range.max ? 'AND p.sales_price <= ?' : ''}
      `
      
      const rangeParams = [...params, range.min]
      if (range.max) rangeParams.push(range.max)
      
      const [result] = await executeQuery(rangeSql, rangeParams)
      return {
        ...range,
        count: result.count
      }
    })

    const rangeResults = await Promise.all(rangePromises)
    return rangeResults.filter(range => range.count > 0)
  }

  /**
   * Facetas de tiendas
   */
  static async getStoreFacets(whereClause, params, filters) {
    if (filters.store_id) return []

    const sql = `
      SELECT 
        p.store_id as id,
        p.store_name as value,
        COUNT(*) as count,
        AVG(p.store_rating) as avg_rating,
        COUNT(CASE WHEN p.has_free_shipping = 1 THEN 1 END) as free_shipping_count
      FROM products p
      ${whereClause}
      GROUP BY p.store_id, p.store_name
      HAVING count >= 3
      ORDER BY count DESC, avg_rating DESC
      LIMIT 20
    `
    
    const results = await executeQuery(sql, params)
    return results.map(row => ({
      id: row.id,
      value: row.value,
      count: row.count,
      avgRating: parseFloat(row.avg_rating),
      freeShippingCount: row.free_shipping_count,
      freeShippingPercentage: Math.round((row.free_shipping_count / row.count) * 100)
    }))
  }

  /**
   * Facetas de fulfillment
   */
  static async getFulfillmentFacets(whereClause, params, filters) {
    if (filters.fulfillment_type) return []

    const sql = `
      SELECT 
        p.fulfillment_type as value,
        COUNT(*) as count,
        AVG(p.shipping_days) as avg_shipping_days
      FROM products p
      ${whereClause}
      GROUP BY p.fulfillment_type
      ORDER BY count DESC
    `
    
    const results = await executeQuery(sql, params)
    return results.map(row => ({
      value: row.value,
      count: row.count,
      avgShippingDays: Math.round(row.avg_shipping_days)
    }))
  }

  /**
   * Facetas de rating
   */
  static async getRatingFacets(whereClause, params, filters) {
    const sql = `
      SELECT 
        CASE 
          WHEN p.review_rating >= 4.5 THEN '4.5+'
          WHEN p.review_rating >= 4.0 THEN '4.0+'
          WHEN p.review_rating >= 3.5 THEN '3.5+'
          WHEN p.review_rating >= 3.0 THEN '3.0+'
          ELSE 'Menos de 3.0'
        END as value,
        COUNT(*) as count,
        AVG(p.total_reviews) as avg_review_count
      FROM products p
      ${whereClause}
      AND p.review_rating IS NOT NULL
      GROUP BY value
      ORDER BY MIN(p.review_rating) DESC
    `
    
    const results = await executeQuery(sql, params)
    return results.map(row => ({
      value: row.value,
      count: row.count,
      avgReviewCount: Math.round(row.avg_review_count)
    }))
  }

  /**
   * Facetas de características especiales
   */
  static async getFeatureFacets(whereClause, params, filters) {
    const features = [
      { key: 'digital', label: 'Producto Digital' },
      { key: 'big_ticket', label: 'Producto de Alto Valor' },
      { key: 'super_express', label: 'Envío Express' },
      { key: 'is_store_pickup', label: 'Recoger en Tienda' },
      { key: 'back_order', label: 'Disponible por Pedido' }
    ]

    const featurePromises = features.map(async (feature) => {
      const sql = `
        SELECT COUNT(*) as count
        FROM products p
        ${whereClause}
        AND p.${feature.key} = 1
      `
      
      const [result] = await executeQuery(sql, params)
      return {
        key: feature.key,
        label: feature.label,
        count: result.count
      }
    })

    const results = await Promise.all(featurePromises)
    return results.filter(feature => feature.count > 0)
  }

  /**
   * Facetas de opciones de envío
   */
  static async getShippingFacets(whereClause, params, filters) {
    const sql = `
      SELECT 
        CASE 
          WHEN p.has_free_shipping = 1 THEN 'Envío Gratis'
          WHEN p.shipping_cost <= 50 THEN 'Envío Económico (≤$50)'
          WHEN p.shipping_cost <= 100 THEN 'Envío Estándar (≤$100)'
          ELSE 'Envío Premium'
        END as value,
        COUNT(*) as count,
        AVG(p.shipping_cost) as avg_cost,
        AVG(p.shipping_days) as avg_days
      FROM products p
      ${whereClause}
      GROUP BY value
      ORDER BY count DESC
    `
    
    const results = await executeQuery(sql, params)
    return results.map(row => ({
      value: row.value,
      count: row.count,
      avgCost: Math.round(row.avg_cost),
      avgDays: Math.round(row.avg_days)
    }))
  }

  /**
   * Genera rangos de precio dinámicos basados en la distribución
   */
  static generatePriceRanges(minPrice, maxPrice, avgPrice) {
    const ranges = []
    
    if (maxPrice <= 1000) {
      // Productos económicos
      ranges.push(
        { label: 'Menos de $200', min: 0, max: 199 },
        { label: '$200 - $499', min: 200, max: 499 },
        { label: '$500 - $999', min: 500, max: 999 },
        { label: '$1,000+', min: 1000, max: null }
      )
    } else if (maxPrice <= 10000) {
      // Rango medio
      ranges.push(
        { label: 'Menos de $1,000', min: 0, max: 999 },
        { label: '$1,000 - $2,999', min: 1000, max: 2999 },
        { label: '$3,000 - $5,999', min: 3000, max: 5999 },
        { label: '$6,000 - $9,999', min: 6000, max: 9999 },
        { label: '$10,000+', min: 10000, max: null }
      )
    } else {
      // Productos premium
      ranges.push(
        { label: 'Menos de $5,000', min: 0, max: 4999 },
        { label: '$5,000 - $14,999', min: 5000, max: 14999 },
        { label: '$15,000 - $29,999', min: 15000, max: 29999 },
        { label: '$30,000 - $49,999', min: 30000, max: 49999 },
        { label: '$50,000+', min: 50000, max: null }
      )
    }
    
    return ranges
  }

  /**
   * Construye condiciones de filtro para WHERE
   */
  static buildFilterConditions(filters) {
    const conditions = []

    if (filters.brand) {
      if (Array.isArray(filters.brand)) {
        conditions.push(`p.brand IN (${filters.brand.map(() => '?').join(',')})`)
      } else {
        conditions.push('p.brand = ?')
      }
    }

    if (filters.store_id) {
      if (Array.isArray(filters.store_id)) {
        conditions.push(`p.store_id IN (${filters.store_id.map(() => '?').join(',')})`)
      } else {
        conditions.push('p.store_id = ?')
      }
    }

    if (filters.min_price) {
      conditions.push('p.sales_price >= ?')
    }

    if (filters.max_price) {
      conditions.push('p.sales_price <= ?')
    }

    if (filters.min_rating) {
      conditions.push('p.review_rating >= ?')
    }

    if (filters.free_shipping === 'true' || filters.free_shipping === true) {
      conditions.push('p.has_free_shipping = 1')
    }

    if (filters.fulfillment_type) {
      conditions.push('p.fulfillment_type = ?')
    }

    if (filters.digital === 'true' || filters.digital === true) {
      conditions.push('p.digital = 1')
    }

    if (filters.has_discount === 'true' || filters.has_discount === true) {
      conditions.push('p.percentage_discount > 0')
    }

    return conditions
  }

  /**
   * Obtiene parámetros para filtros
   */
  static getFilterParams(filters) {
    const params = []

    if (filters.brand) {
      if (Array.isArray(filters.brand)) {
        params.push(...filters.brand)
      } else {
        params.push(filters.brand)
      }
    }

    if (filters.store_id) {
      if (Array.isArray(filters.store_id)) {
        params.push(...filters.store_id.map(id => parseInt(id)))
      } else {
        params.push(parseInt(filters.store_id))
      }
    }

    if (filters.min_price) params.push(parseFloat(filters.min_price))
    if (filters.max_price) params.push(parseFloat(filters.max_price))
    if (filters.min_rating) params.push(parseFloat(filters.min_rating))
    if (filters.fulfillment_type) params.push(filters.fulfillment_type)

    return params
  }

  /**
   * Obtiene filtros aplicados para mostrar en UI
   */
  static getAppliedFilters(filters) {
    const applied = []

    if (filters.brand) {
      const brands = Array.isArray(filters.brand) ? filters.brand : [filters.brand]
      brands.forEach(brand => {
        applied.push({ type: 'brand', value: brand, label: `Marca: ${brand}` })
      })
    }

    if (filters.store_id) {
      const stores = Array.isArray(filters.store_id) ? filters.store_id : [filters.store_id]
      stores.forEach(storeId => {
        applied.push({ type: 'store_id', value: storeId, label: `Tienda: ${storeId}` })
      })
    }

    if (filters.min_price || filters.max_price) {
      const priceLabel = filters.min_price && filters.max_price 
        ? `Precio: $${filters.min_price} - $${filters.max_price}`
        : filters.min_price 
          ? `Precio: Desde $${filters.min_price}`
          : `Precio: Hasta $${filters.max_price}`
      
      applied.push({ 
        type: 'price_range', 
        value: { min: filters.min_price, max: filters.max_price }, 
        label: priceLabel 
      })
    }

    if (filters.min_rating) {
      applied.push({ 
        type: 'min_rating', 
        value: filters.min_rating, 
        label: `Rating: ${filters.min_rating}+ estrellas` 
      })
    }

    if (filters.free_shipping === 'true' || filters.free_shipping === true) {
      applied.push({ type: 'free_shipping', value: true, label: 'Envío Gratis' })
    }

    if (filters.fulfillment_type) {
      applied.push({ 
        type: 'fulfillment_type', 
        value: filters.fulfillment_type, 
        label: `Fulfillment: ${filters.fulfillment_type}` 
      })
    }

    if (filters.digital === 'true' || filters.digital === true) {
      applied.push({ type: 'digital', value: true, label: 'Producto Digital' })
    }

    if (filters.has_discount === 'true' || filters.has_discount === true) {
      applied.push({ type: 'has_discount', value: true, label: 'Con Descuento' })
    }

    return applied
  }

  /**
   * Obtiene el conteo total de productos
   */
  static async getTotalProductCount(whereClause, params) {
    const sql = `SELECT COUNT(*) as total FROM products p ${whereClause}`
    const [result] = await executeQuery(sql, params)
    return result.total
  }

  /**
   * Actualiza las facetas pre-calculadas en segundo plano
   */
  static async updatePreCalculatedFacets() {
    try {
      await executeQuery('CALL UpdateAllFacets()')
      console.log('✅ Pre-calculated facets updated successfully')
    } catch (error) {
      console.error('❌ Error updating pre-calculated facets:', error)
      throw error
    }
  }

  /**
   * Obtiene facetas rápidas desde tabla pre-calculada
   */
  static async getQuickFacets(categoryId = null) {
    try {
      let sql = `
        SELECT facet_type, facet_value, facet_count
        FROM facet_counts 
        WHERE facet_count > 0
      `
      const params = []
      
      if (categoryId) {
        sql += ` AND (category_id = ? OR category_id IS NULL)`
        params.push(categoryId)
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
      
      return facets
    } catch (error) {
      console.error('Error getting quick facets:', error)
      throw error
    }
  }
}