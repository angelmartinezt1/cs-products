// scripts/typesense-indexer.js
import axios from 'axios'
import fs from 'fs/promises'
import { performance } from 'perf_hooks'

// Configuración de Typesense
const TYPESENSE_CONFIG = {
  host: process.env.TYPESENSE_HOST || 'localhost',
  port: process.env.TYPESENSE_PORT || '8108',
  protocol: process.env.TYPESENSE_PROTOCOL || 'http',
  apiKey: process.env.TYPESENSE_API_KEY || 'cs-products-search-supersecret-key-2024',
  collectionName: process.env.TYPESENSE_COLLECTION || 'products'
}

// Configuración de la API
const API_CONFIG = {
  baseUrl: 'https://csapi.claroshop.com/products/v1/products/',
  pageSize: 100, // Más conservador para Typesense
  maxRetries: 3,
  retryDelay: 1000,
  timeout: 30000
}

class TypesenseIndexer {
  constructor () {
    this.baseUrl = `${TYPESENSE_CONFIG.protocol}://${TYPESENSE_CONFIG.host}:${TYPESENSE_CONFIG.port}`
    this.headers = {
      'X-TYPESENSE-API-KEY': TYPESENSE_CONFIG.apiKey,
      'Content-Type': 'application/json'
    }

    this.stats = {
      totalProducts: 0,
      processedProducts: 0,
      indexedProducts: 0,
      failedProducts: 0,
      batches: 0,
      errors: [],
      startTime: null,
      endTime: null
    }

    this.batchSize = 50 // Batches para Typesense
    this.logFile = `logs/typesense-indexer-${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.log`
  }

  async init () {
    console.log('🚀 Iniciando Typesense Product Indexer...')
    this.stats.startTime = performance.now()

    // Crear directorio de logs si no existe
    await this.ensureLogDirectory()

    // Inicializar archivo de log
    await this.writeLog('INFO', 'Indexer iniciado')
    await this.writeLog('INFO', `Configuración: ${JSON.stringify(TYPESENSE_CONFIG, null, 2)}`)

    try {
      // Verificar conexión con Typesense
      await this.checkTypesenseConnection()

      // Verificar/crear colección
      await this.ensureCollection()

      console.log('✅ Typesense configurado correctamente')
    } catch (error) {
      await this.writeLog('ERROR', `Error en inicialización: ${error.message}`)
      throw error
    }
  }

  async ensureLogDirectory () {
    try {
      await fs.mkdir('logs', { recursive: true })
    } catch (error) {
      // Directorio ya existe o error menor
    }
  }

  async writeLog (level, message, data = null) {
    const timestamp = new Date().toISOString()
    const logEntry = {
      timestamp,
      level,
      message,
      ...(data && { data })
    }

    const logLine = JSON.stringify(logEntry) + '\n'

    try {
      await fs.appendFile(this.logFile, logLine)
    } catch (error) {
      console.error('Error escribiendo log:', error.message)
    }

    // También mostrar en consola
    const emoji = level === 'ERROR' ? '❌' : level === 'WARN' ? '⚠️' : level === 'SUCCESS' ? '✅' : 'ℹ️'
    console.log(`${emoji} [${level}] ${message}`)
  }

  async checkTypesenseConnection () {
    try {
      const response = await axios.get(`${this.baseUrl}/health`, {
        headers: { 'X-TYPESENSE-API-KEY': this.headers['X-TYPESENSE-API-KEY'] }
      })

      if (response.data.ok !== true) {
        throw new Error('Typesense no está disponible')
      }

      await this.writeLog('SUCCESS', 'Conexión con Typesense establecida')
    } catch (error) {
      throw new Error(`Error conectando con Typesense: ${error.message}`)
    }
  }

  async ensureCollection () {
    try {
      // Verificar si la colección existe
      await axios.get(`${this.baseUrl}/collections/${TYPESENSE_CONFIG.collectionName}`, {
        headers: this.headers
      })

      await this.writeLog('INFO', `Colección '${TYPESENSE_CONFIG.collectionName}' ya existe`)
    } catch (error) {
      if (error.response?.status === 404) {
        // Crear la colección
        await this.createCollection()
      } else {
        throw error
      }
    }
  }

  async createCollection () {
    const schema = {
      name: TYPESENSE_CONFIG.collectionName,
      fields: [
        // IDs
        { name: 'objectID', type: 'string' },
        { name: 'product_id', type: 'int32' },
        { name: 'external_id', type: 'string' },

        // Información básica
        { name: 'title', type: 'string' },
        { name: 'title_seo', type: 'string', optional: true },
        { name: 'description', type: 'string', optional: true },
        { name: 'short_description', type: 'string', optional: true },
        { name: 'sku', type: 'string', optional: true },
        { name: 'ean', type: 'string', optional: true },
        { name: 'brand', type: 'string', facet: true, optional: true },

        // Precios
        { name: 'price', type: 'float' },
        { name: 'sale_price', type: 'float' },
        { name: 'percent_off', type: 'int32', optional: true },

        // Stock y estado
        { name: 'stock', type: 'int32' },
        { name: 'is_active', type: 'bool' },

        // Categorías jerárquicas
        { name: 'hirerarchical_category.lvl0', type: 'string', facet: true, optional: true },
        { name: 'hirerarchical_category.lvl1', type: 'string', facet: true, optional: true },
        { name: 'hirerarchical_category.lvl2', type: 'string', facet: true, optional: true },

        // Seller/Store
        { name: 'store_rating', type: 'int32', optional: true },

        // Características del producto
        { name: 'fulfillment', type: 'bool', facet: true },
        { name: 'has_free_shipping', type: 'bool', facet: true },
        { name: 'store_only', type: 'bool', facet: true },
        { name: 'store_pickup', type: 'bool', facet: true },
        { name: 'super_express', type: 'bool', facet: true },
        { name: 'digital', type: 'bool', facet: true },

        // Reviews y ratings
        { name: 'review_rating', type: 'float', optional: true },
        { name: 'total_reviews', type: 'int32', optional: true },

        // Relevancia y metadatos
        { name: 'relevance_score', type: 'float' },
        { name: 'relevance_sales', type: 'int32', optional: true },
        { name: 'relevance_amount', type: 'int32', optional: true },
        { name: 'indexing_date', type: 'int64' },

        // División
        { name: 'division', type: 'int32', facet: true, optional: true }
      ],
      default_sorting_field: 'relevance_score'
    }

    try {
      await axios.post(`${this.baseUrl}/collections`, schema, {
        headers: this.headers
      })

      await this.writeLog('SUCCESS', `Colección '${TYPESENSE_CONFIG.collectionName}' creada exitosamente`)
    } catch (error) {
      throw new Error(`Error creando colección: ${error.message}`)
    }
  }

  async fetchProductsFromAPI (page = 1) {
    const url = `${API_CONFIG.baseUrl}?page_size=${API_CONFIG.pageSize}&page=${page}`

    for (let attempt = 1; attempt <= API_CONFIG.maxRetries; attempt++) {
      try {
        await this.writeLog('INFO', `Obteniendo página ${page} (intento ${attempt})`)

        const response = await axios.get(url, {
          timeout: API_CONFIG.timeout,
          headers: {
            'User-Agent': 'TypesenseIndexer/1.0',
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
        await this.writeLog('WARN', `Intento ${attempt} falló para página ${page}: ${error.message}`)

        if (attempt === API_CONFIG.maxRetries) {
          throw new Error(`Failed after ${API_CONFIG.maxRetries} attempts: ${error.message}`)
        }

        await this.sleep(API_CONFIG.retryDelay * attempt)
      }
    }
  }

  transformProductForTypesense (product) {
    try {
      // Procesar categorías jerárquicas
      const categories = this.parseCategories(product.categories)

      // Calcular relevancia
      const relevanceScore = this.calculateRelevanceScore(product)

      // Mapear el documento para Typesense
      const document = {
        objectID: String(product.id || product.external_id),
        product_id: parseInt(product.id || product.external_id),
        external_id: String(product.external_id || product.id),

        // Información básica
        title: (product.title || '').substring(0, 500),
        title_seo: product.title_seo || null,
        description: product.description || null,
        short_description: product.short_description ? product.short_description.substring(0, 1000) : null,
        sku: product.sku || null,
        ean: product.ean || null,
        brand: product.brand || null,

        // Precios
        price: product.pricing?.list_price || 0,
        sale_price: product.pricing?.sales_price || product.pricing?.sale_price || 0,
        percent_off: product.pricing?.percentage_discount || 0,

        // Stock y estado
        stock: product.stock || 0,
        is_active: Boolean(product.is_active),

        // Categorías jerárquicas
        'hirerarchical_category.lvl0': categories.lvl0,
        'hirerarchical_category.lvl1': categories.lvl1,
        'hirerarchical_category.lvl2': categories.lvl2,

        // Seller/Store
        store_rating: product.seller?.store_rating || 0,

        // Características del producto
        fulfillment: Boolean(product.features?.super_express || product.features?.fulfillment_id),
        has_free_shipping: Boolean(product.shipping?.is_free || product.shipping?.free_shipping),
        store_only: Boolean(product.is_store_only || product.features?.is_store_only),
        store_pickup: Boolean(product.is_store_pickup || product.features?.is_store_pickup),
        super_express: Boolean(product.features?.super_express),
        digital: Boolean(product.features?.digital),

        // Reviews y ratings
        review_rating: product.rating?.average_score || product.rating?.average || 0,
        total_reviews: product.rating?.total_reviews || product.rating?.count || 0,

        // Relevancia y metadatos
        relevance_score: relevanceScore,
        relevance_sales: product.relevance_sales || 0,
        relevance_amount: product.relevance_amount || 0,
        indexing_date: Math.floor(Date.now() / 1000),

        // División
        division: product.division || 1
      }

      return document
    } catch (error) {
      throw new Error(`Error transformando producto ${product.id}: ${error.message}`)
    }
  }

  parseCategories (categories) {
    if (!categories || !Array.isArray(categories) || categories.length === 0) {
      return { lvl0: null, lvl1: null, lvl2: null }
    }

    // Obtener la primera categoría y ordenar por nivel
    const mainCategory = Array.isArray(categories[0]) ? categories[0] : categories
    const sortedCategories = [...mainCategory].sort((a, b) => (b.level || 0) - (a.level || 0))

    // Convertir a minúsculas
    const lvl0 = sortedCategories.find(cat => cat.level === 2)?.name?.toLowerCase() || null
    const lvl1 = sortedCategories.find(cat => cat.level === 1)?.name?.toLowerCase() || null
    const lvl2 = sortedCategories.find(cat => cat.level === 0)?.name?.toLowerCase() || null

    // Construir jerarquía
    const hierarchicalLvl0 = lvl0
    let hierarchicalLvl1 = lvl0
    let hierarchicalLvl2 = lvl0

    if (lvl1 && lvl1 !== lvl0) {
      hierarchicalLvl1 = `${lvl0} > ${lvl1}`
      hierarchicalLvl2 = `${lvl0} > ${lvl1}`
    }

    if (lvl2 && lvl2 !== lvl1) {
      hierarchicalLvl2 = `${hierarchicalLvl1} > ${lvl2}`
    }

    return {
      lvl0: hierarchicalLvl0,
      lvl1: hierarchicalLvl1,
      lvl2: hierarchicalLvl2
    }
  }

  calculateRelevanceScore (product) {
    let score = 0

    // Stock
    const stock = product.stock || 0
    if (stock > 0) {
      score += Math.min(stock * 0.3, 10)
    }

    // Rating
    const avgRating = product.rating?.average_score || 0
    if (avgRating > 0) {
      score += avgRating * 3
    }

    // Reviews
    const totalReviews = product.rating?.total_reviews || 0
    if (totalReviews > 0) {
      score += Math.min(Math.log10(totalReviews + 1) * 3, 10)
    }

    // Descuento
    const discount = product.pricing?.percentage_discount || 0
    if (discount > 0) {
      score += Math.min(discount * 0.16, 8)
    }

    // Super express
    if (product.features?.super_express === true) {
      score += 10
    }

    // Envío gratis
    if (product.shipping?.is_free === true) {
      score += 7
    }

    // Producto activo
    if (product.is_active === true) {
      score += 5
    }

    // Relevancia API
    const apiRelevanceSales = product.relevance_sales || 0
    const apiRelevanceAmount = product.relevance_amount || 0

    if (apiRelevanceSales > 0) {
      score += Math.min(apiRelevanceSales * 0.15, 12)
    }

    if (apiRelevanceAmount > 0) {
      score += Math.min(apiRelevanceAmount * 0.08, 8)
    }

    return Math.min(Math.round(score * 100) / 100, 100.00)
  }

  async indexProductBatch (products) {
    if (!products || products.length === 0) {
      return { indexed: 0, failed: 0, errors: [] }
    }

    try {
      // Transformar productos
      const documents = []
      const transformErrors = []

      for (const product of products) {
        try {
          const doc = this.transformProductForTypesense(product)
          documents.push(doc)
        } catch (error) {
          transformErrors.push({
            productId: product.id || 'unknown',
            error: error.message
          })
          await this.writeLog('ERROR', `Error transformando producto ${product.id}`, { error: error.message })
        }
      }

      if (documents.length === 0) {
        return { indexed: 0, failed: products.length, errors: transformErrors }
      }

      // Indexar en Typesense usando import
      const importData = documents.map(doc => JSON.stringify(doc)).join('\n')

      await this.writeLog('INFO', `Enviando batch de ${documents.length} documentos a Typesense`)

      const response = await axios.post(
        `${this.baseUrl}/collections/${TYPESENSE_CONFIG.collectionName}/documents/import?action=upsert`,
        importData,
        {
          headers: {
            ...this.headers,
            'Content-Type': 'application/jsonl'
          },
          timeout: 60000
        }
      )

      // Log para debugging
      await this.writeLog('INFO', 'Respuesta de Typesense recibida', {
        responseType: typeof response.data,
        isArray: Array.isArray(response.data),
        dataLength: response.data?.length || 'N/A',
        sampleData: typeof response.data === 'string' ? response.data.substring(0, 200) : JSON.stringify(response.data).substring(0, 200)
      })

      // Procesar respuesta - manejar tanto JSONL como JSON array
      let results = []

      if (typeof response.data === 'string') {
        // Formato JSONL (newline-delimited JSON)
        results = response.data.split('\n').filter(line => line.trim()).map(line => {
          try {
            return JSON.parse(line)
          } catch (e) {
            return { success: false, error: 'Invalid JSON line' }
          }
        })
      } else if (Array.isArray(response.data)) {
        // Formato JSON array directo
        results = response.data
      } else {
        // Respuesta única o formato inesperado
        results = [response.data]
      }

      let indexed = 0
      let failed = 0
      const indexErrors = []

      for (let i = 0; i < results.length; i++) {
        const result = results[i]

        if (result && result.success === true) {
          indexed++
        } else {
          failed++
          indexErrors.push({
            productId: documents[i]?.product_id || 'unknown',
            error: result?.error || result?.message || 'Unknown indexing error',
            fullError: result // Para debugging
          })
        }
      }

      return {
        indexed,
        failed: failed + transformErrors.length,
        errors: [...transformErrors, ...indexErrors]
      }
    } catch (error) {
      await this.writeLog('ERROR', `Error en batch de ${products.length} productos`, { error: error.message })
      return {
        indexed: 0,
        failed: products.length,
        errors: [{ error: error.message, products: products.length }]
      }
    }
  }

  async indexAllProducts () {
    let currentPage = 1
    let hasMorePages = true

    console.log('📊 Iniciando indexación completa en Typesense...')
    await this.writeLog('INFO', 'Iniciando indexación completa')

    while (hasMorePages) {
      try {
        const apiResponse = await this.fetchProductsFromAPI(currentPage)

        if (!apiResponse.success || !apiResponse.products.length) {
          await this.writeLog('INFO', 'No hay más productos para procesar')
          break
        }

        await this.writeLog('INFO', `Procesando ${apiResponse.products.length} productos de la página ${currentPage}`)

        // Procesar en batches
        const batches = this.chunkArray(apiResponse.products, this.batchSize)

        for (let i = 0; i < batches.length; i++) {
          const batch = batches[i]
          const batchStart = performance.now()

          const result = await this.indexProductBatch(batch)

          // Actualizar estadísticas
          this.stats.processedProducts += batch.length
          this.stats.indexedProducts += result.indexed
          this.stats.failedProducts += result.failed
          this.stats.batches++
          this.stats.errors.push(...result.errors)

          const batchTime = (performance.now() - batchStart) / 1000
          const speed = (batch.length / batchTime).toFixed(2)

          await this.writeLog('SUCCESS',
            `Batch ${i + 1}/${batches.length} completado: ${result.indexed} indexados, ${result.failed} fallidos en ${batchTime.toFixed(2)}s (${speed} p/s)`
          )

          // Pausa entre batches
          if (i < batches.length - 1) {
            await this.sleep(200)
          }
        }

        // Verificar paginación
        if (apiResponse.pagination) {
          this.stats.totalProducts = apiResponse.pagination.totalItemCount || 0
          hasMorePages = currentPage < (apiResponse.pagination.pageCount || 0)
        } else {
          hasMorePages = false
        }

        currentPage++
        await this.sleep(100)
      } catch (error) {
        await this.writeLog('ERROR', `Error procesando página ${currentPage}`, { error: error.message })
        this.stats.errors.push({ page: currentPage, error: error.message })

        currentPage++
        await this.sleep(2000)
      }
    }
  }

  async generateReport () {
    this.stats.endTime = performance.now()
    const duration = (this.stats.endTime - this.stats.startTime) / 1000

    const report = {
      summary: {
        duration: `${duration.toFixed(2)} segundos`,
        totalProducts: this.stats.totalProducts,
        processedProducts: this.stats.processedProducts,
        indexedProducts: this.stats.indexedProducts,
        failedProducts: this.stats.failedProducts,
        batches: this.stats.batches,
        speed: `${(this.stats.processedProducts / duration).toFixed(2)} productos/seg`,
        successRate: `${((this.stats.indexedProducts / this.stats.processedProducts) * 100).toFixed(2)}%`
      },
      errors: this.stats.errors,
      timestamp: new Date().toISOString()
    }

    // Guardar reporte detallado
    const reportFile = `logs/typesense-report-${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.json`
    await fs.writeFile(reportFile, JSON.stringify(report, null, 2))

    // Log del resumen
    await this.writeLog('INFO', 'RESUMEN FINAL', report.summary)

    console.log('\n📊 RESUMEN DE INDEXACIÓN TYPESENSE:')
    console.log('='.repeat(50))
    console.log(`⏱️  Duración: ${report.summary.duration}`)
    console.log(`📦 Total de productos: ${report.summary.totalProducts}`)
    console.log(`✅ Productos indexados: ${report.summary.indexedProducts}`)
    console.log(`❌ Productos fallidos: ${report.summary.failedProducts}`)
    console.log(`📊 Lotes procesados: ${report.summary.batches}`)
    console.log(`🚀 Velocidad: ${report.summary.speed}`)
    console.log(`📈 Tasa de éxito: ${report.summary.successRate}`)
    console.log(`📄 Reporte: ${reportFile}`)
    console.log(`📋 Log: ${this.logFile}`)
    console.log('='.repeat(50))

    return report
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
}

// Función principal
async function main () {
  const indexer = new TypesenseIndexer()

  try {
    await indexer.init()
    await indexer.indexAllProducts()
    await indexer.generateReport()
  } catch (error) {
    console.error('💥 Error fatal:', error.message)
    await indexer.writeLog('ERROR', 'Error fatal', { error: error.message })
    process.exit(1)
  }
}

// Ejecutar si se llama directamente
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error)
}

export { TypesenseIndexer }
