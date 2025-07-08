// services/UltraSimpleBulkService.js
import { performance } from 'perf_hooks'

class UltraSimpleBulkService {
  constructor (connection) {
    this.connection = connection
    this.batchSize = 20
    this.maxRetries = 3
  }

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

        const chunkResult = await this.processProductChunkUltraSimple(chunk)

        results.inserted += chunkResult.inserted
        results.updated += chunkResult.updated
        results.errors += chunkResult.errors

        const chunkTime = performance.now() - chunkStart
        const speed = (chunk.length / (chunkTime / 1000)).toFixed(2)

        console.log(`✅ Lote ${i + 1} completado: ${chunkResult.inserted} insertados, ${chunkResult.updated} actualizados en ${(chunkTime / 1000).toFixed(2)}s (${speed} p/s)`)
      } catch (error) {
        console.error(`❌ Error en lote ${i + 1}:`, error.message)
        results.errors += chunk.length
      }

      // Pausa entre lotes
      if (i < chunks.length - 1) {
        await this.sleep(100)
      }
    }

    return results
  }

  async processProductChunkUltraSimple (products) {
    let inserted = 0
    let updated = 0
    let errors = 0

    try {
      await this.connection.execute('START TRANSACTION')

      for (const product of products) {
        try {
          // Primero verificar si el producto existe
          const [existingProduct] = await this.connection.execute(
            'SELECT id FROM products WHERE id = ?',
            [product.id]
          )

          if (existingProduct.length > 0) {
            // UPDATE - producto existe
            const updateSql = `
              UPDATE products SET
                name = ?, description = ?, short_description = ?, sku = ?, brand = ?,
                sales_price = ?, list_price = ?, shipping_cost = ?, percentage_discount = ?,
                stock = ?, status = ?, visible = ?,
                category_id = ?, category_name = ?, category_lvl0 = ?, category_lvl1 = ?, category_lvl2 = ?, category_path = ?,
                store_id = ?, store_name = ?, store_logo = ?, store_rating = ?, store_authorized = ?,
                digital = ?, big_ticket = ?, back_order = ?, is_store_pickup = ?, super_express = ?, is_store_only = ?, shipping_days = ?,
                review_rating = ?, total_reviews = ?,
                main_image = ?, thumbnail = ?, fulfillment_type = ?, relevance_score = ?,
                updated_at = CURRENT_TIMESTAMP
              WHERE id = ?
            `

            await this.connection.execute(updateSql, [
              product.name, product.description, product.short_description, product.sku, product.brand,
              product.sales_price, product.list_price, product.shipping_cost, product.percentage_discount,
              product.stock, product.status, product.visible,
              product.category_id, product.category_name, product.category_lvl0, product.category_lvl1, product.category_lvl2, product.category_path,
              product.store_id, product.store_name, product.store_logo, product.store_rating, product.store_authorized,
              product.digital, product.big_ticket, product.back_order, product.is_store_pickup, product.super_express, product.is_store_only, product.shipping_days,
              product.review_rating, product.total_reviews,
              product.main_image, product.thumbnail, product.fulfillment_type, product.relevance_score,
              product.id
            ])

            updated++
          } else {
            // INSERT - producto nuevo
            const insertSql = `
              INSERT INTO products (
                id, name, description, short_description, sku, brand,
                sales_price, list_price, shipping_cost, percentage_discount,
                stock, status, visible,
                category_id, category_name, category_lvl0, category_lvl1, category_lvl2, category_path,
                store_id, store_name, store_logo, store_rating, store_authorized,
                digital, big_ticket, back_order, is_store_pickup, super_express, is_store_only, shipping_days,
                review_rating, total_reviews,
                main_image, thumbnail, fulfillment_type, relevance_score
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `

            await this.connection.execute(insertSql, [
              product.id, product.name, product.description, product.short_description, product.sku, product.brand,
              product.sales_price, product.list_price, product.shipping_cost, product.percentage_discount,
              product.stock, product.status, product.visible,
              product.category_id, product.category_name, product.category_lvl0, product.category_lvl1, product.category_lvl2, product.category_path,
              product.store_id, product.store_name, product.store_logo, product.store_rating, product.store_authorized,
              product.digital, product.big_ticket, product.back_order, product.is_store_pickup, product.super_express, product.is_store_only, product.shipping_days,
              product.review_rating, product.total_reviews,
              product.main_image, product.thumbnail, product.fulfillment_type, product.relevance_score
            ])

            inserted++
          }

          // Procesar imágenes si existen
          await this.processProductImages(product)
        } catch (productError) {
          console.warn(`⚠️ Error procesando producto ${product.id}: ${productError.message}`)
          errors++
        }
      }

      await this.connection.execute('COMMIT')
    } catch (error) {
      await this.connection.execute('ROLLBACK')
      throw error
    }

    return { inserted, updated, errors }
  }

  async processProductImages (product) {
    if (!product.originalData?.pictures || product.originalData.pictures.length <= 1) {
      return
    }

    // Limpiar imágenes existentes
    try {
      await this.connection.execute(
        'DELETE FROM product_images WHERE product_id = ?',
        [product.id]
      )
    } catch (error) {
      // Ignorar errores de limpieza
    }

    // Insertar imágenes adicionales (saltando la primera)
    const insertImageSql = `
      INSERT INTO product_images (product_id, image_url, thumbnail_url, image_order)
      VALUES (?, ?, ?, ?)
    `

    for (let i = 1; i < product.originalData.pictures.length; i++) {
      const picture = product.originalData.pictures[i]
      if (picture.source) {
        try {
          await this.connection.execute(insertImageSql, [
            product.id,
            picture.source,
            picture.thumbnail || picture.source,
            i + 1
          ])
        } catch (error) {
          // Ignorar errores de imagen individual
        }
      }
    }
  }

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
        SELECT COUNT(*) as total_images FROM product_images
      `)

      const [variationStats] = await this.connection.execute(`
        SELECT COUNT(*) as total_variations FROM product_variations
      `)

      const [attributeStats] = await this.connection.execute(`
        SELECT COUNT(*) as total_attributes FROM product_attributes
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

  async updateFacets () {
    try {
      console.log('🔄 Actualizando facetas...')
      await this.connection.execute('CALL UpdateAllFacets()')
      console.log('✅ Facetas actualizadas')
      return { success: true }
    } catch (error) {
      console.error('❌ Error actualizando facetas:', error.message)
      throw error
    }
  }

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

  chunkArray (array, size) {
    const chunks = []
    for (let i = 0; i < array.length; i += size) {
      chunks.push(array.slice(i, i + size))
    }
    return chunks
  }

  sleep (ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
  }

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

  filterValidProducts (products) {
    return products.filter(product => this.validateProduct(product))
  }
}

export default UltraSimpleBulkService
