// scripts/product-indexer.js
import axios from 'axios'
import mysql from 'mysql2/promise'
import { performance } from 'perf_hooks'

// Configuración de la API
const API_CONFIG = {
  baseUrl: 'https://csapi.claroshop.com/products/v1/products/',
  pageSize: 299, // Incrementar para mejor rendimiento
  maxRetries: 3,
  retryDelay: 1000,
  timeout: 30000
}

// Configuración de la base de datos
const DB_CONFIG = {
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME || 'search_products',
  charset: 'utf8mb4',
  multipleStatements: true,
  timeout: 60000
}

class ProductIndexer {
  constructor () {
    this.connection = null
    this.stats = {
      totalProducts: 0,
      processedProducts: 0,
      insertedProducts: 0,
      updatedProducts: 0,
      skippedProducts: 0,
      errors: 0,
      batches: 0,
      startTime: null,
      endTime: null
    }
  }

  async init () {
    console.log('🚀 Iniciando indexación de productos...')
    this.stats.startTime = performance.now()

    try {
      this.connection = await mysql.createConnection(DB_CONFIG)
      console.log('✅ Conexión a base de datos establecida')
    } catch (error) {
      console.error('❌ Error conectando a la base de datos:', error.message)
      throw error
    }
  }

  async fetchProductsFromAPI (page = 1) {
    const url = `${API_CONFIG.baseUrl}?page_size=${API_CONFIG.pageSize}&page=${page}`

    for (let attempt = 1; attempt <= API_CONFIG.maxRetries; attempt++) {
      try {
        console.log(`📡 Obteniendo página ${page} (intento ${attempt})...`)

        const response = await axios.get(url, {
          timeout: API_CONFIG.timeout,
          headers: {
            'User-Agent': 'ProductIndexer/1.0',
            Accept: 'application/json'
          }
        })

        if (response.data && response.data.metadata && response.data.metadata.is_error === false) {
          return {
            products: response.data.data || [],
            pagination: response.data.pagination || null,
            success: true
          }
        } else {
          throw new Error(`API error: ${response.data?.metadata?.message || 'Unknown error'}`)
        }
      } catch (error) {
        console.warn(`⚠️  Intento ${attempt} falló:`, error.message)

        if (attempt === API_CONFIG.maxRetries) {
          throw new Error(`Failed after ${API_CONFIG.maxRetries} attempts: ${error.message}`)
        }

        // Esperar antes del siguiente intento
        await this.sleep(API_CONFIG.retryDelay * attempt)
      }
    }
  }

  parseCategories (categories) {
    if (!categories || !Array.isArray(categories) || categories.length === 0) {
      return {
        category_id: null,
        category_name: null,
        category_lvl0: null,
        category_lvl1: null,
        category_lvl2: null,
        category_path: null
      }
    }

    // Tomar la primera categoría (principal)
    const mainCategory = categories[0]
    if (!Array.isArray(mainCategory) || mainCategory.length === 0) {
      return {
        category_id: null,
        category_name: null,
        category_lvl0: null,
        category_lvl1: null,
        category_lvl2: null,
        category_path: null
      }
    }

    // Ordenar por nivel (level) para construir la jerarquía correcta
    const sortedCategories = [...mainCategory].sort((a, b) => (b.level || 0) - (a.level || 0))

    // ✅ CONVERTIR A MINÚSCULAS todas las categorías
    const lvl0 = sortedCategories.find(cat => cat.level === 2)?.name?.toLowerCase() || null
    const lvl1 = sortedCategories.find(cat => cat.level === 1)?.name?.toLowerCase() || null
    const lvl2 = sortedCategories.find(cat => cat.level === 0)?.name?.toLowerCase() || null

    // Construir el path jerárquico
    const path = []
    if (lvl0) path.push(lvl0)
    if (lvl1 && lvl1 !== lvl0) path.push(lvl1)
    if (lvl2 && lvl2 !== lvl1) path.push(lvl2)

    // Construir category_lvl1 y category_lvl2 con formato "padre > hijo"
    let category_lvl1_formatted = lvl0
    let category_lvl2_formatted = lvl0

    if (lvl1 && lvl1 !== lvl0) {
      category_lvl1_formatted = `${lvl0} > ${lvl1}`
      category_lvl2_formatted = `${lvl0} > ${lvl1}`
    }

    if (lvl2 && lvl2 !== lvl1) {
      category_lvl2_formatted = `${category_lvl1_formatted} > ${lvl2}`
    }

    return {
      category_id: sortedCategories[0]?.id || null,
      category_name: lvl2 || lvl1 || lvl0, // ✅ Ya está en minúsculas
      category_lvl0: lvl0, // ✅ Ya está en minúsculas
      category_lvl1: category_lvl1_formatted, // ✅ Ya está en minúsculas
      category_lvl2: category_lvl2_formatted, // ✅ Ya está en minúsculas
      category_path: path.join(' > ') // ✅ Ya está en minúsculas
    }
  }

  transformProduct (product) {
    const categories = this.parseCategories(product.categories)
    const mainImage = product.pictures && product.pictures.length > 0 ? product.pictures[0] : null
    const volumetry = product.volumetries && product.volumetries.length > 0 ? product.volumetries[0] : {}

    return {
      // IDs
      id: product.id || product.external_id,

      // Información básica
      name: (product.title || '').substring(0, 500),
      description: product.description || null,
      short_description: product.short_description ? product.short_description.substring(0, 1000) : null,
      sku: product.sku || null,
      brand: product.brand || null,

      // Precios
      sales_price: product.pricing?.sales_price || 0,
      list_price: product.pricing?.list_price || 0,
      shipping_cost: product.shipping?.cost || 0,
      percentage_discount: product.pricing?.percentage_discount || 0,

      // Stock y disponibilidad
      stock: product.stock || 0,
      status: product.is_active ? 1 : 0,
      visible: 1, // Por defecto visible

      // Categorías
      ...categories,

      // Tienda/Seller
      store_id: product.seller?.id || null,
      store_name: product.seller?.name || null,
      store_logo: product.seller?.logo || null,
      store_rating: product.seller?.store_rating || null,
      store_authorized: product.seller?.status ? 1 : 0,

      // Características del producto
      digital: product.features?.digital ? 1 : 0,
      big_ticket: product.features?.is_big_ticket ? 1 : 0,
      back_order: product.features?.is_backorder ? 1 : 0,
      is_store_pickup: product.features?.is_store_pickup ? 1 : 0,
      super_express: product.features?.super_express ? 1 : 0,
      is_store_only: product.features?.is_store_only ? 1 : 0,
      shipping_days: product.shipping?.days || 5,

      // Reviews y ratings
      review_rating: product.rating?.average_score || null,
      total_reviews: product.rating?.total_reviews || 0,

      // Imágenes
      main_image: mainImage?.source || null,
      thumbnail: mainImage?.thumbnail || null,

      // Fulfillment
      fulfillment_type: product.features?.fulfillment_id ? 'fulfillment' : 'seller',

      // Metadatos
      relevance_score: product.relevance_sales || 0,

      // Guardar datos originales para imágenes/variaciones
      originalData: product
    }
  }

  async bulkInsertProducts (products) {
    if (!products || products.length === 0) return { inserted: 0, updated: 0, errors: 0 }

    const transformedProducts = products.map(p => this.transformProduct(p))
    const chunkSize = 100 // Reducido para mejor compatibilidad
    const results = { inserted: 0, updated: 0, errors: 0 }

    for (let i = 0; i < transformedProducts.length; i += chunkSize) {
      const chunk = transformedProducts.slice(i, i + chunkSize)
      const chunkResult = await this.processProductChunk(chunk)

      results.inserted += chunkResult.inserted
      results.updated += chunkResult.updated
      results.errors += chunkResult.errors
    }

    return results
  }

  async processProductChunk (products) {
    try {
      await this.connection.execute('START TRANSACTION')

      // Paso 1: Insertar productos usando múltiples VALUES
      const productResult = await this.insertProductsBatch(products)

      // Paso 2: Procesar imágenes adicionales
      await this.bulkInsertImages(products)

      // Paso 3: Procesar variaciones si existen
      await this.bulkInsertVariations(products)

      await this.connection.execute('COMMIT')

      return {
        inserted: productResult.inserted,
        updated: productResult.updated,
        errors: 0
      }
    } catch (error) {
      await this.connection.execute('ROLLBACK')
      console.error('❌ Error en chunk de productos:', error.message)
      return { inserted: 0, updated: 0, errors: products.length }
    }
  }

  async insertProductsBatch (products) {
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

  async bulkInsertImages (products) {
    // Limpiar imágenes existentes
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
        product.originalData.pictures.slice(1).forEach((picture, index) => {
          if (picture.source) {
            imageValues.push('(?, ?, ?, ?)')
            imageParams.push(
              product.id,
              picture.source,
              picture.thumbnail,
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

  async bulkInsertVariations (products) {
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
              variation.size,
              variation.color,
              variation.stock || 0,
              0 // price_modifier
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

  async indexAllProducts () {
    let currentPage = 1
    let hasMorePages = true

    console.log('📊 Iniciando indexación completa de productos...')

    while (hasMorePages) {
      try {
        const apiResponse = await this.fetchProductsFromAPI(currentPage)

        if (!apiResponse.success || !apiResponse.products.length) {
          console.log('✅ No hay más productos para procesar')
          break
        }

        console.log(`📦 Procesando ${apiResponse.products.length} productos de la página ${currentPage}...`)

        // Insertar productos en la BD
        const insertResult = await this.bulkInsertProducts(apiResponse.products)

        // Actualizar estadísticas
        this.stats.processedProducts += apiResponse.products.length
        this.stats.insertedProducts += insertResult.inserted
        this.stats.updatedProducts += insertResult.updated
        this.stats.errors += insertResult.errors
        this.stats.batches++

        console.log(`✅ Página ${currentPage}: ${insertResult.inserted} insertados, ${insertResult.updated} actualizados, ${insertResult.errors} errores`)

        // Verificar si hay más páginas
        if (apiResponse.pagination) {
          this.stats.totalProducts = apiResponse.pagination.totalItemCount || 0
          hasMorePages = currentPage < (apiResponse.pagination.pageCount || 0)
        } else {
          // Si no hay info de paginación, asumir que terminamos
          hasMorePages = false
        }

        currentPage++

        // Pausa pequeña para no sobrecargar la API
        await this.sleep(100)
      } catch (error) {
        console.error(`❌ Error procesando página ${currentPage}:`, error.message)
        this.stats.errors++

        // Continuar con la siguiente página en caso de error
        currentPage++
        await this.sleep(2000) // Pausa más larga en caso de error
      }
    }
  }

  async updateFacets () {
    console.log('🔄 Actualizando facetas...')
    try {
      await this.connection.execute('CALL UpdateAllFacets()')
      console.log('✅ Facetas actualizadas')
    } catch (error) {
      console.error('❌ Error actualizando facetas:', error.message)
    }
  }

  printStats () {
    this.stats.endTime = performance.now()
    const duration = (this.stats.endTime - this.stats.startTime) / 1000

    console.log('\n📊 RESUMEN DE INDEXACIÓN:')
    console.log('================================')
    console.log(`⏱️  Duración: ${duration.toFixed(2)} segundos`)
    console.log(`📦 Total de productos: ${this.stats.totalProducts}`)
    console.log(`✅ Productos procesados: ${this.stats.processedProducts}`)
    console.log(`➕ Productos insertados: ${this.stats.insertedProducts}`)
    console.log(`🔄 Productos actualizados: ${this.stats.updatedProducts}`)
    console.log(`⚠️  Productos omitidos: ${this.stats.skippedProducts}`)
    console.log(`❌ Errores: ${this.stats.errors}`)
    console.log(`📊 Lotes procesados: ${this.stats.batches}`)
    console.log(`🚀 Velocidad: ${(this.stats.processedProducts / duration).toFixed(2)} productos/seg`)
    console.log('================================')
  }

  sleep (ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
  }

  async close () {
    if (this.connection) {
      await this.connection.end()
      console.log('🔌 Conexión a base de datos cerrada')
    }
  }
}

// Función principal
async function main () {
  const indexer = new ProductIndexer()

  try {
    await indexer.init()
    await indexer.indexAllProducts()
    await indexer.updateFacets()
    indexer.printStats()
  } catch (error) {
    console.error('💥 Error fatal:', error.message)
    process.exit(1)
  } finally {
    await indexer.close()
  }
}

// Ejecutar si se llama directamente
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error)
}

export { ProductIndexer }
