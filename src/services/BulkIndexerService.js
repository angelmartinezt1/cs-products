// services/BulkIndexerService.js
import { performance } from 'perf_hooks'

class BulkIndexerService {
  constructor (connection) {
    this.connection = connection
    this.batchSize = 100 // Reducido para mejor compatibilidad
    this.maxRetries = 3
  }

  /**
   * Inserta o actualiza productos masivamente
   * @param {Array} products - Array de productos transformados
   * @returns {Object} Resultado de la operación
   */
  async bulkUpsertProducts (products) {
    if (!products || products.length === 0) {
      return { inserted: 0, updated: 0, errors: 0, details: [] }
    }

    const chunks = this.chunkArray(products, this.batchSize)
    const results = {
      inserted: 0,
      updated: 0,
      errors: 0,
      details: []
    }

    console.log(`📦 Procesando ${products.length} productos en ${chunks.length} lotes de ${this.batchSize}`)

    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i]
      const chunkStart = performance.now()

      try {
        console.log(`⚙️  Procesando lote ${i + 1}/${chunks.length} (${chunk.length} productos)...`)

        const chunkResult = await this.processProductChunk(chunk)

        results.inserted += chunkResult.inserted
        results.updated += chunkResult.updated
        results.errors += chunkResult.errors

        const chunkTime = performance.now() - chunkStart
        const speed = (chunk.length / (chunkTime / 1000)).toFixed(2)

        console.log(`✅ Lote ${i + 1} completado: ${chunkResult.inserted} insertados, ${chunkResult.updated} actualizados en ${(chunkTime / 1000).toFixed(2)}s (${speed} p/s)`)

        results.details.push({
          batch: i + 1,
          products: chunk.length,
          inserted: chunkResult.inserted,
          updated: chunkResult.updated,
          errors: chunkResult.errors,
          processingTime: chunkTime / 1000,
          speed: parseFloat(speed)
        })
      } catch (error) {
        console.error(`❌ Error en lote ${i + 1}:`, error.message)
        results.errors += chunk.length
        results.details.push({
          batch: i + 1,
          products: chunk.length,
          inserted: 0,
          updated: 0,
          errors: chunk.length,
          error: error.message
        })
      }

      // Pequeña pausa entre lotes para no sobrecargar la DB
      if (i < chunks.length - 1) {
        await this.sleep(50)
      }
    }

    return results
  }

  /**
   * Procesa un chunk de productos con transacción
   * @param {Array} products - Chunk de productos
   * @returns {Object} Resultado del chunk
   */
  async processProductChunk (products) {
    let attempt = 0

    while (attempt < this.maxRetries) {
      try {
        await this.connection.execute('START TRANSACTION')

        // Paso 1: Insertar/actualizar productos principales
        const productResult = await this.upsertProducts(products)

        // Paso 2: Procesar imágenes adicionales
        await this.upsertProductImages(products)

        // Paso 3: Procesar variaciones/tallas
        await this.upsertProductVariations(products)

        // Paso 4: Procesar atributos si existen
        await this.upsertProductAttributes(products)

        await this.connection.execute('COMMIT')

        return {
          inserted: productResult.inserted,
          updated: productResult.updated,
          errors: 0
        }
      } catch (error) {
        await this.connection.execute('ROLLBACK')
        attempt++

        if (attempt >= this.maxRetries) {
          throw new Error(`Failed after ${this.maxRetries} attempts: ${error.message}`)
        }

        console.warn(`⚠️  Reintentando lote (intento ${attempt + 1}/${this.maxRetries}): ${error.message}`)
        await this.sleep(1000 * attempt) // Backoff exponencial
      }
    }
  }

  /**
   * Inserta o actualiza productos en la tabla principal usando múltiples VALUES
   * @param {Array} products - Array de productos
   * @returns {Object} Resultado de la operación
   */
  async upsertProducts (products) {
    // Construir query con múltiples VALUES usando placeholders
    const values = []
    const params = []

    for (const product of products) {
      values.push('(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')
      params.push(
        product.id, product.name, product.description, product.short_description, product.sku, product.brand,
        product.sales_price, product.list_price, product.shipping_cost, product.percentage_discount,
        product.stock, product.status, product.visible,
        product.category_id, product.category_name, product.category_lvl0, product.category_lvl1, product.category_lvl2, product.category_path,
        product.store_id, product.store_name, product.store_logo, product.store_rating, product.store_authorized,
        product.digital, product.big_ticket, product.back_order, product.is_store_pickup, product.super_express, product.is_store_only, product.shipping_days,
        product.review_rating, product.total_reviews,
        product.main_image, product.thumbnail, product.fulfillment_type, product.relevance_score
      )
    }

    const sql = `
      INSERT INTO products (
        id, name, description, short_description, sku, brand,
        sales_price, list_price, shipping_cost, percentage_discount,
        stock, status, visible,
        category_id, category_name, category_lvl0, category_lvl1, category_lvl2, category_path,
        store_id, store_name, store_logo, store_rating, store_authorized,
        digital, big_ticket, back_order, is_store_pickup, super_express, is_store_only, shipping_days,
        review_rating, total_reviews,
        main_image, thumbnail, fulfillment_type, relevance_score
      ) VALUES ${values.join(', ')}
      ON DUPLICATE KEY UPDATE
        name = VALUES(name),
        description = VALUES(description),
        short_description = VALUES(short_description),
        sku = VALUES(sku),
        brand = VALUES(brand),
        sales_price = VALUES(sales_price),
        list_price = VALUES(list_price),
        shipping_cost = VALUES(shipping_cost),
        percentage_discount = VALUES(percentage_discount),
        stock = VALUES(stock),
        status = VALUES(status),
        category_id = VALUES(category_id),
        category_name = VALUES(category_name),
        category_lvl0 = VALUES(category_lvl0),
        category_lvl1 = VALUES(category_lvl1),
        category_lvl2 = VALUES(category_lvl2),
        category_path = VALUES(category_path),
        store_id = VALUES(store_id),
        store_name = VALUES(store_name),
        store_logo = VALUES(store_logo),
        store_rating = VALUES(store_rating),
        store_authorized = VALUES(store_authorized),
        digital = VALUES(digital),
        big_ticket = VALUES(big_ticket),
        back_order = VALUES(back_order),
        is_store_pickup = VALUES(is_store_pickup),
        super_express = VALUES(super_express),
        is_store_only = VALUES(is_store_only),
        shipping_days = VALUES(shipping_days),
        review_rating = VALUES(review_rating),
        total_reviews = VALUES(total_reviews),
        main_image = VALUES(main_image),
        thumbnail = VALUES(thumbnail),
        fulfillment_type = VALUES(fulfillment_type),
        relevance_score = VALUES(relevance_score),
        updated_at = CURRENT_TIMESTAMP
    `

    const [result] = await this.connection.execute(sql, params)

    return {
      inserted: result.affectedRows - result.changedRows,
      updated: result.changedRows
    }
  }

  /**
   * Inserta imágenes adicionales de productos usando múltiples VALUES
   * @param {Array} products - Array de productos con datos originales
   */
  async upsertProductImages (products) {
    // Primero limpiar imágenes existentes para evitar duplicados
    const productIds = products.map(p => p.id).filter(Boolean)
    if (productIds.length > 0) {
      const placeholders = productIds.map(() => '?').join(',')
      await this.connection.execute(
        `DELETE FROM product_images WHERE product_id IN (${placeholders})`,
        productIds
      )
    }

    // Recopilar todas las imágenes
    const imageValues = []
    const imageParams = []

    products.forEach(product => {
      if (product.originalData?.pictures && Array.isArray(product.originalData.pictures) && product.originalData.pictures.length > 1) {
        // Saltar la primera imagen (ya está en main_image)
        product.originalData.pictures.slice(1).forEach((picture, index) => {
          if (picture.source) {
            imageValues.push('(?, ?, ?, ?)')
            imageParams.push(
              product.id,
              picture.source,
              picture.thumbnail || picture.source,
              index + 2 // Orden 2, 3, 4...
            )
          }
        })
      }
    })

    if (imageValues.length > 0) {
      const sql = `
        INSERT INTO product_images (product_id, image_url, thumbnail_url, image_order)
        VALUES ${imageValues.join(', ')}
      `
      await this.connection.execute(sql, imageParams)
    }
  }

  /**
   * Inserta variaciones de productos usando múltiples VALUES
   * @param {Array} products - Array de productos con datos originales
   */
  async upsertProductVariations (products) {
    // Limpiar variaciones existentes
    const productIds = products.map(p => p.id).filter(Boolean)
    if (productIds.length > 0) {
      const placeholders = productIds.map(() => '?').join(',')
      await this.connection.execute(
        `DELETE FROM product_variations WHERE product_id IN (${placeholders})`,
        productIds
      )
    }

    // Recopilar todas las variaciones
    const variationValues = []
    const variationParams = []

    products.forEach(product => {
      if (product.originalData?.variations && Array.isArray(product.originalData.variations)) {
        product.originalData.variations.forEach(variation => {
          if (variation.sku) {
            variationValues.push('(?, ?, ?, ?, ?, ?)')
            variationParams.push(
              product.id,
              variation.sku,
              variation.size || null,
              variation.color || null,
              variation.stock || 0,
              0 // price_modifier por defecto
            )
          }
        })
      }
    })

    if (variationValues.length > 0) {
      const sql = `
        INSERT INTO product_variations (product_id, sku, size_name, color_name, stock, price_modifier)
        VALUES ${variationValues.join(', ')}
      `
      await this.connection.execute(sql, variationParams)
    }
  }

  /**
   * Inserta atributos de productos usando múltiples VALUES
   * @param {Array} products - Array de productos con datos originales
   */
  async upsertProductAttributes (products) {
    // Limpiar atributos existentes
    const productIds = products.map(p => p.id).filter(Boolean)
    if (productIds.length > 0) {
      const placeholders = productIds.map(() => '?').join(',')
      await this.connection.execute(
        `DELETE FROM product_attributes WHERE product_id IN (${placeholders})`,
        productIds
      )
    }

    // Recopilar todos los atributos
    const attributeValues = []
    const attributeParams = []

    products.forEach(product => {
      // Atributos regulares
      if (product.originalData?.attributes && typeof product.originalData.attributes === 'object') {
        Object.entries(product.originalData.attributes).forEach(([key, value]) => {
          if (value !== null && value !== undefined && value !== '') {
            attributeValues.push('(?, ?, ?)')
            attributeParams.push(
              product.id,
              key.substring(0, 100), // Limitar longitud del nombre
              String(value).substring(0, 500) // Limitar longitud del valor
            )
          }
        })
      }

      // También extraer atributos de volumetría si existen
      if (product.originalData?.volumetries && Array.isArray(product.originalData.volumetries) && product.originalData.volumetries.length > 0) {
        const vol = product.originalData.volumetries[0]
        const volumeAttrs = [
          ['height', vol.height],
          ['width', vol.width],
          ['depth', vol.depth],
          ['weight', vol.weight],
          ['volumetric_weight', vol.volumetric_weight]
        ]

        volumeAttrs.forEach(([name, value]) => {
          if (value) {
            attributeValues.push('(?, ?, ?)')
            attributeParams.push(product.id, name, String(value))
          }
        })
      }
    })

    if (attributeValues.length > 0) {
      const sql = `
        INSERT INTO product_attributes (product_id, attribute_name, attribute_value)
        VALUES ${attributeValues.join(', ')}
      `
      await this.connection.execute(sql, attributeParams)
    }
  }

  /**
   * Actualiza facetas pre-calculadas
   * @returns {Object} Resultado de la actualización
   */
  async updateFacets () {
    const startTime = performance.now()

    try {
      console.log('🔄 Actualizando facetas pre-calculadas...')

      // Llamar al procedimiento almacenado que actualiza todas las facetas
      await this.connection.execute('CALL UpdateAllFacets()')

      const endTime = performance.now()
      const duration = (endTime - startTime) / 1000

      console.log(`✅ Facetas actualizadas en ${duration.toFixed(2)} segundos`)

      return {
        success: true,
        duration,
        timestamp: new Date().toISOString()
      }
    } catch (error) {
      console.error('❌ Error actualizando facetas:', error.message)
      throw error
    }
  }

  /**
   * Obtiene estadísticas de la base de datos
   * @returns {Object} Estadísticas
   */
  async getDatabaseStats () {
    try {
      const [productStats] = await this.connection.execute(`
        SELECT 
          COUNT(*) as total_products,
          COUNT(CASE WHEN status = 1 AND visible = 1 THEN 1 END) as active_products,
          COUNT(DISTINCT brand) as unique_brands,
          COUNT(DISTINCT store_id) as unique_stores,
          COUNT(DISTINCT category_id) as unique_categories,
          AVG(sales_price) as avg_price,
          SUM(stock) as total_stock
        FROM products
      `)

      const [imageStats] = await this.connection.execute(`
        SELECT COUNT(*) as total_images
        FROM product_images
      `)

      const [variationStats] = await this.connection.execute(`
        SELECT COUNT(*) as total_variations
        FROM product_variations
      `)

      const [attributeStats] = await this.connection.execute(`
        SELECT COUNT(*) as total_attributes
        FROM product_attributes
      `)

      const [facetStats] = await this.connection.execute(`
        SELECT 
          COUNT(*) as total_facets,
          MAX(last_updated) as last_facet_update
        FROM facet_counts
      `)

      return {
        products: productStats[0],
        images: imageStats[0],
        variations: variationStats[0],
        attributes: attributeStats[0],
        facets: facetStats[0],
        timestamp: new Date().toISOString()
      }
    } catch (error) {
      console.error('❌ Error obteniendo estadísticas:', error.message)
      throw error
    }
  }

  /**
   * Limpia productos inactivos o antiguos
   * @param {number} daysOld - Días de antigüedad para considerar productos antiguos
   * @returns {Object} Resultado de la limpieza
   */
  async cleanupOldProducts (daysOld = 30) {
    try {
      console.log(`🧹 Limpiando productos inactivos con más de ${daysOld} días...`)

      const [result] = await this.connection.execute(`
        DELETE FROM products 
        WHERE status = 0 
          AND updated_at < DATE_SUB(NOW(), INTERVAL ? DAY)
      `, [daysOld])

      console.log(`✅ ${result.affectedRows} productos antiguos eliminados`)

      return {
        deleted: result.affectedRows,
        daysOld,
        timestamp: new Date().toISOString()
      }
    } catch (error) {
      console.error('❌ Error en limpieza:', error.message)
      throw error
    }
  }

  /**
   * Optimiza las tablas de la base de datos
   * @returns {Object} Resultado de la optimización
   */
  async optimizeTables () {
    const tables = ['products', 'product_images', 'product_variations', 'product_attributes', 'facet_counts']
    const results = []

    console.log('⚡ Optimizando tablas...')

    for (const table of tables) {
      try {
        const startTime = performance.now()
        await this.connection.execute(`OPTIMIZE TABLE ${table}`)
        const duration = performance.now() - startTime

        console.log(`✅ Tabla ${table} optimizada en ${(duration / 1000).toFixed(2)}s`)
        results.push({ table, success: true, duration: duration / 1000 })
      } catch (error) {
        console.error(`❌ Error optimizando tabla ${table}:`, error.message)
        results.push({ table, success: false, error: error.message })
      }
    }

    return {
      results,
      timestamp: new Date().toISOString()
    }
  }

  /**
   * Divide un array en chunks
   * @param {Array} array - Array a dividir
   * @param {number} size - Tamaño de cada chunk
   * @returns {Array} Array de chunks
   */
  chunkArray (array, size) {
    const chunks = []
    for (let i = 0; i < array.length; i += size) {
      chunks.push(array.slice(i, i + size))
    }
    return chunks
  }

  /**
   * Pausa la ejecución por un tiempo determinado
   * @param {number} ms - Milisegundos a esperar
   * @returns {Promise} Promise que se resuelve después del tiempo especificado
   */
  sleep (ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
  }

  /**
   * Valida la estructura de un producto
   * @param {Object} product - Producto a validar
   * @returns {boolean} True si es válido
   */
  validateProduct (product) {
    return (
      product &&
      product.id &&
      product.name &&
      typeof product.id === 'number' &&
      typeof product.name === 'string' &&
      product.name.length > 0
    )
  }

  /**
   * Filtra productos válidos
   * @param {Array} products - Array de productos
   * @returns {Array} Array de productos válidos
   */
  filterValidProducts (products) {
    return products.filter(product => this.validateProduct(product))
  }
}

export default BulkIndexerService
