// scripts/typesense-indexer-checkpoint.js
import axios from 'axios'
import { program } from 'commander'
import { existsSync, readFileSync } from 'fs'
import fs from 'fs/promises'
import { performance } from 'perf_hooks'
import Typesense from 'typesense'

// Configuración CLI
program
  .option('-p, --pages <number>', 'Número máximo de páginas a procesar', '0')
  .option('-s, --start-page <number>', 'Página inicial', '1')
  .option('-b, --batch-size <number>', 'Tamaño de lote', '25')
  .option('--resume', 'Reanudar desde último checkpoint', false)
  .option('--checkpoint-interval <number>', 'Guardar checkpoint cada N páginas', '10')
  .option('--debug-errors', 'Modo debug detallado para errores', false)
  .option('--stop-on-errors', 'Detener al encontrar errores', false)
  .option('--max-consecutive-errors <number>', 'Máximo errores consecutivos antes de parar', '5')
  .parse()

const options = program.opts()

// Configuración de Typesense
const TYPESENSE_CONFIG = {
  nodes: [{
    host: process.env.TYPESENSE_HOST || 'localhost',
    port: process.env.TYPESENSE_PORT || '8108',
    protocol: process.env.TYPESENSE_PROTOCOL || 'http'
  }],
  apiKey: process.env.TYPESENSE_API_KEY || 'cs-products-search-supersecret-key-2024',
  connectionTimeoutSeconds: 60,
  collectionName: process.env.TYPESENSE_COLLECTION || 'products'
}

// Configuración de la API
const API_CONFIG = {
  baseUrl: 'https://csapi.claroshop.com/products/v1/products/',
  pageSize: 299,
  maxRetries: 3,
  retryDelay: 1000,
  timeout: 30000
}

class TypesenseIndexerCheckpoint {
  constructor () {
    this.client = new Typesense.Client(TYPESENSE_CONFIG)
    this.collectionName = TYPESENSE_CONFIG.collectionName

    this.stats = {
      totalProducts: 0,
      processedProducts: 0,
      indexedProducts: 0,
      failedProducts: 0,
      batches: 0,
      errors: [],
      startTime: null,
      endTime: null,
      lastSuccessfulPage: 0,
      consecutiveErrors: 0
    }

    this.batchSize = parseInt(options.batchSize) || 25
    this.maxPages = parseInt(options.pages) || 0
    this.startPage = parseInt(options.startPage) || 1
    this.checkpointInterval = parseInt(options.checkpointInterval) || 10
    this.debugErrors = options.debugErrors || false
    this.stopOnErrors = options.stopOnErrors || false
    this.maxConsecutiveErrors = parseInt(options.maxConsecutiveErrors) || 5

    this.timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, '-')
    this.logFile = `logs/typesense-checkpoint-${this.timestamp}.log`
    this.checkpointFile = `logs/checkpoint-${this.collectionName}.json`
    this.errorSamplesFile = `logs/error-samples-${this.timestamp}.json`

    this.shouldStop = false
    this.errorSamples = []
  }

  async init () {
    console.log('🚀 Iniciando Typesense Indexer con Checkpoint...')

    // Manejar señales para parada elegante
    this.setupSignalHandlers()

    // Resumir desde checkpoint si se solicita
    if (options.resume) {
      await this.loadCheckpoint()
    }

    console.log('📋 Configuración:')
    console.log(`  📄 Páginas: ${this.maxPages || 'todas'}`)
    console.log(`  🔢 Desde página: ${this.startPage}`)
    console.log(`  📦 Batch size: ${this.batchSize}`)
    console.log(`  💾 Checkpoint cada: ${this.checkpointInterval} páginas`)
    console.log(`  🐛 Debug errores: ${this.debugErrors}`)
    console.log(`  🛑 Parar en errores: ${this.stopOnErrors}`)

    this.stats.startTime = performance.now()

    await this.ensureLogDirectory()
    await this.writeLog('INFO', 'Indexer con checkpoint iniciado', {
      configuration: {
        maxPages: this.maxPages,
        startPage: this.startPage,
        batchSize: this.batchSize,
        checkpointInterval: this.checkpointInterval,
        debugErrors: this.debugErrors,
        stopOnErrors: this.stopOnErrors
      }
    })

    try {
      await this.checkConnection()
      await this.validateExistingSchema()
      console.log('✅ Configuración validada')
    } catch (error) {
      await this.writeLog('ERROR', `Error en inicialización: ${error.message}`)
      throw error
    }
  }

  setupSignalHandlers () {
    // Manejar Ctrl+C elegantemente
    process.on('SIGINT', async () => {
      console.log('\n🛑 Señal de interrupción recibida. Guardando checkpoint...')
      this.shouldStop = true
      await this.saveCheckpoint()
      await this.writeLog('INFO', 'Proceso interrumpido por usuario')
      process.exit(0)
    })

    // Manejar SIGTERM
    process.on('SIGTERM', async () => {
      console.log('\n🛑 Señal SIGTERM recibida. Guardando checkpoint...')
      this.shouldStop = true
      await this.saveCheckpoint()
      await this.writeLog('INFO', 'Proceso terminado por SIGTERM')
      process.exit(0)
    })
  }

  async loadCheckpoint () {
    try {
      if (existsSync(this.checkpointFile)) {
        const checkpoint = JSON.parse(readFileSync(this.checkpointFile, 'utf8'))

        this.startPage = checkpoint.lastSuccessfulPage + 1
        this.stats = { ...this.stats, ...checkpoint.stats }

        console.log(`📂 Checkpoint cargado: reanudando desde página ${this.startPage}`)
        console.log(`📊 Estadísticas previas: ${checkpoint.stats.indexedProducts} indexados, ${checkpoint.stats.failedProducts} fallidos`)

        await this.writeLog('INFO', 'Checkpoint cargado', checkpoint)
      } else {
        console.log('📂 No se encontró checkpoint anterior')
      }
    } catch (error) {
      console.log(`⚠️ Error cargando checkpoint: ${error.message}`)
    }
  }

  async saveCheckpoint () {
    try {
      const checkpoint = {
        timestamp: new Date().toISOString(),
        lastSuccessfulPage: this.stats.lastSuccessfulPage,
        stats: this.stats,
        configuration: {
          maxPages: this.maxPages,
          batchSize: this.batchSize,
          collectionName: this.collectionName
        }
      }

      await fs.writeFile(this.checkpointFile, JSON.stringify(checkpoint, null, 2))
      await this.writeLog('INFO', `Checkpoint guardado: página ${this.stats.lastSuccessfulPage}`)
    } catch (error) {
      await this.writeLog('ERROR', `Error guardando checkpoint: ${error.message}`)
    }
  }

  async saveErrorSample (product, error, context = {}) {
    if (this.debugErrors) {
      const errorSample = {
        timestamp: new Date().toISOString(),
        productId: product?.id || 'unknown',
        productTitle: product?.title || 'unknown',
        error: error.message || error,
        context,
        productSample: {
          id: product?.id,
          title: product?.title?.substring(0, 100),
          brand: product?.brand,
          pricing: product?.pricing,
          categories: product?.categories
        }
      }

      this.errorSamples.push(errorSample)

      // Guardar samples cada 10 errores
      if (this.errorSamples.length % 10 === 0) {
        await fs.writeFile(this.errorSamplesFile, JSON.stringify(this.errorSamples, null, 2))
      }
    }
  }

  async ensureLogDirectory () {
    try {
      await fs.mkdir('logs', { recursive: true })
    } catch (error) {
      // Directorio ya existe
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

    const emoji = level === 'ERROR' ? '❌' : level === 'WARN' ? '⚠️' : level === 'SUCCESS' ? '✅' : 'ℹ️'
    console.log(`${emoji} [${level}] ${message}`)
  }

  async checkConnection () {
    try {
      const health = await this.client.health.retrieve()
      if (health.ok !== true) {
        throw new Error('Typesense no está disponible')
      }
      await this.writeLog('SUCCESS', 'Conexión establecida')
    } catch (error) {
      throw new Error(`Error conectando: ${error.message}`)
    }
  }

  async validateExistingSchema () {
    try {
      const collection = await this.client.collections(this.collectionName).retrieve()
      await this.writeLog('INFO', `Esquema validado: ${collection.fields?.length || 0} campos`)

      // Verificar campos obligatorios
      const requiredFields = collection.fields.filter(f => !f.optional).map(f => f.name)
      await this.writeLog('INFO', `Campos obligatorios: ${requiredFields.join(', ')}`)

      return collection
    } catch (error) {
      throw new Error(`Error validando esquema: ${error.message}`)
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
            'User-Agent': 'TypesenseCheckpointIndexer/1.0',
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
        await this.writeLog('WARN', `Intento ${attempt} falló: ${error.message}`)

        if (attempt === API_CONFIG.maxRetries) {
          throw new Error(`Failed after ${API_CONFIG.maxRetries} attempts: ${error.message}`)
        }

        await this.sleep(API_CONFIG.retryDelay * attempt)
      }
    }
  }

  transformProductForTypesense (product) {
    try {
      const categories = this.parseCategories(product.categories)
      const relevanceScore = this.calculateRelevanceScore(product)

      const document = {
        // Campos obligatorios
        objectID: String(product.id || product.external_id),
        product_id: parseInt(product.id || product.external_id),
        external_id: String(product.external_id || product.id),
        title: (product.title || '').substring(0, 500),
        title_seo: product.title_seo || this.generateTitleSeo(product.title),
        stock: product.stock || 0,
        is_active: Boolean(product.is_active),
        sale_price: product.pricing?.sales_price || product.pricing?.sale_price || 0,
        indexing_date: Math.floor(Date.now() / 1000),
        relevance_score: relevanceScore,

        // Campos opcionales con validación
        ean: product.ean || null,
        sku: product.sku || null,
        division: product.division || 1,
        brand: product.brand || null,
        description: product.description || null,
        short_description: product.short_description ? product.short_description.substring(0, 1000) : null,
        updated_at: product.updated_at || new Date().toISOString(),
        created_at: product.created_at || new Date().toISOString(),
        relevance_sales: product.relevance_sales || 0,
        relevance_amount: product.relevance_amount || 0,
        wallet: Boolean(product.wallet),
        home: Boolean(product.home),
        cs_months: Array.isArray(product.cs_months) ? product.cs_months : [],

        // Objetos complejos con validación
        pictures: Array.isArray(product.pictures) ? product.pictures : [],
        variations: product.variations && typeof product.variations === 'object' ? product.variations : {},
        attributes: Array.isArray(product.attributes) ? product.attributes : [],
        videos: Array.isArray(product.videos) ? product.videos : [],
        volumetries: Array.isArray(product.volumetries) ? product.volumetries : [],
        seller: product.seller || null,
        categories: Array.isArray(product.categories) ? product.categories : [],
        features: product.features && typeof product.features === 'object' ? product.features : {},

        // Campos booleanos
        is_store_only: Boolean(product.is_store_only || product.features?.is_store_only),
        is_store_pickup: Boolean(product.is_store_pickup || product.features?.is_store_pickup),
        is_backorder: Boolean(product.features?.is_backorder),
        is_big_ticket: Boolean(product.features?.is_big_ticket),
        super_express: Boolean(product.features?.super_express),
        digital: Boolean(product.features?.digital),

        // Otros campos
        presale_date: product.presale_date || null,
        fulfillment_id: product.features?.fulfillment_id || null,
        extended_catalogue_days: product.extended_catalogue_days || null,

        // Objetos validados
        pricing: product.pricing && typeof product.pricing === 'object' ? product.pricing : {},
        shipping: product.shipping && typeof product.shipping === 'object' ? product.shipping : {},
        warranties: product.warranties && typeof product.warranties === 'object' ? product.warranties : {},
        rating: product.rating && typeof product.rating === 'object' ? product.rating : {},

        // Campos calculados
        price: product.pricing?.list_price || 0,
        percent_off: product.pricing?.percentage_discount || 0,
        fulfillment: Boolean(product.features?.super_express || product.features?.fulfillment_id),
        has_free_shipping: Boolean(product.shipping?.is_free || product.shipping?.free_shipping),
        store_only: Boolean(product.is_store_only || product.features?.is_store_only),
        store_pickup: Boolean(product.is_store_pickup || product.features?.is_store_pickup),

        // Fotos
        photos: Array.isArray(product.pictures) ? product.pictures : [],

        // Categorías jerárquicas
        hirerarchical_category: {
          lvl0: categories.lvl0,
          lvl1: categories.lvl1,
          lvl2: categories.lvl2
        },
        'hirerarchical_category.lvl0': categories.lvl0,
        'hirerarchical_category.lvl1': categories.lvl1,
        'hirerarchical_category.lvl2': categories.lvl2,

        // Sellers
        sellers: product.seller ? [product.seller] : [],

        // Campos de rating
        review_rating: product.rating?.average_score || product.rating?.average || 0,
        total_reviews: product.rating?.total_reviews || product.rating?.count || 0,
        store_rating: product.seller?.store_rating || 0,

        // Campos adicionales
        ccs_months: Array.isArray(product.ccs_months) ? product.ccs_months : [],
        fecha_alta_cms: product.fecha_alta_cms || Math.floor(Date.now() / 1000),
        temporada: product.temporada || 0
      }

      return document
    } catch (error) {
      throw new Error(`Error transformando producto ${product.id}: ${error.message}`)
    }
  }

  generateTitleSeo (title) {
    if (!title) return 'producto'

    return title
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, '')
      .replace(/\s+/g, '-')
      .substring(0, 100)
  }

  parseCategories (categories) {
    if (!categories || !Array.isArray(categories) || categories.length === 0) {
      return { lvl0: null, lvl1: null, lvl2: null }
    }

    try {
      const mainCategory = Array.isArray(categories[0]) ? categories[0] : categories
      const sortedCategories = [...mainCategory].sort((a, b) => (b.level || 0) - (a.level || 0))

      const lvl0 = sortedCategories.find(cat => cat.level === 2)?.name?.toLowerCase() || null
      const lvl1 = sortedCategories.find(cat => cat.level === 1)?.name?.toLowerCase() || null
      const lvl2 = sortedCategories.find(cat => cat.level === 0)?.name?.toLowerCase() || null

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
    } catch (error) {
      return { lvl0: null, lvl1: null, lvl2: null }
    }
  }

  calculateRelevanceScore (product) {
    let score = 0

    try {
      const stock = product.stock || 0
      if (stock > 0) {
        score += Math.min(stock * 0.3, 10)
      }

      const avgRating = product.rating?.average_score || 0
      if (avgRating > 0) {
        score += avgRating * 3
      }

      const totalReviews = product.rating?.total_reviews || 0
      if (totalReviews > 0) {
        score += Math.min(Math.log10(totalReviews + 1) * 3, 10)
      }

      const discount = product.pricing?.percentage_discount || 0
      if (discount > 0) {
        score += Math.min(discount * 0.16, 8)
      }

      if (product.features?.super_express === true) score += 10
      if (product.shipping?.is_free === true) score += 7
      if (product.is_active === true) score += 5

      const apiRelevanceSales = product.relevance_sales || 0
      const apiRelevanceAmount = product.relevance_amount || 0

      if (apiRelevanceSales > 0) {
        score += Math.min(apiRelevanceSales * 0.15, 12)
      }

      if (apiRelevanceAmount > 0) {
        score += Math.min(apiRelevanceAmount * 0.08, 8)
      }

      return Math.min(Math.round(score * 100) / 100, 100.00)
    } catch (error) {
      return 50.0 // Score por defecto en caso de error
    }
  }

  async indexProductBatch (products) {
    if (!products || products.length === 0) {
      return { indexed: 0, failed: 0, errors: [] }
    }

    try {
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

          await this.saveErrorSample(product, error, { phase: 'transformation' })
          await this.writeLog('ERROR', `Error transformando producto ${product.id}`, { error: error.message })
        }
      }

      if (documents.length === 0) {
        return { indexed: 0, failed: products.length, errors: transformErrors }
      }

      await this.writeLog('INFO', `Indexando batch de ${documents.length} documentos`)

      const results = await this.client.collections(this.collectionName).documents().import(documents, {
        action: 'upsert'
      })

      let indexed = 0
      let failed = 0
      const indexErrors = []

      for (let i = 0; i < results.length; i++) {
        const result = results[i]

        if (result.success === true) {
          indexed++
          this.stats.consecutiveErrors = 0 // Reset contador de errores
        } else {
          failed++
          this.stats.consecutiveErrors++

          indexErrors.push({
            productId: documents[i]?.product_id || 'unknown',
            error: result.error || 'Unknown indexing error'
          })

          await this.saveErrorSample(documents[i], result.error, {
            phase: 'indexing',
            resultCode: result.code
          })

          if (this.debugErrors) {
            await this.writeLog('ERROR', `Error indexando producto ${documents[i]?.product_id}`, {
              error: result.error,
              document: documents[i]?.title || 'unknown',
              fullResult: result
            })
          }
        }
      }

      await this.writeLog('SUCCESS', `Batch indexado: ${indexed} exitosos, ${failed} fallidos`)

      return {
        indexed,
        failed: failed + transformErrors.length,
        errors: [...transformErrors, ...indexErrors]
      }
    } catch (error) {
      this.stats.consecutiveErrors++
      await this.writeLog('ERROR', `Error en batch de ${products.length} productos`, { error: error.message })

      // Guardar sample del primer producto para debugging
      if (products.length > 0) {
        await this.saveErrorSample(products[0], error, { phase: 'batch_processing' })
      }

      return {
        indexed: 0,
        failed: products.length,
        errors: [{ error: error.message, products: products.length }]
      }
    }
  }

  async indexAllProducts () {
    let currentPage = this.startPage
    let hasMorePages = true
    let pagesProcessed = 0

    console.log('📊 Iniciando indexación completa con checkpoint...')
    await this.writeLog('INFO', 'Iniciando indexación completa')

    while (hasMorePages && !this.shouldStop) {
      // Verificar límite de páginas
      if (this.maxPages > 0 && pagesProcessed >= this.maxPages) {
        console.log(`🛑 Límite de páginas alcanzado (${this.maxPages} páginas)`)
        break
      }

      // Verificar errores consecutivos
      if (this.stats.consecutiveErrors >= this.maxConsecutiveErrors) {
        console.log(`🛑 Demasiados errores consecutivos (${this.stats.consecutiveErrors}). Deteniendo...`)
        await this.writeLog('ERROR', `Proceso detenido por ${this.stats.consecutiveErrors} errores consecutivos`)
        break
      }

      try {
        const apiResponse = await this.fetchProductsFromAPI(currentPage)

        if (!apiResponse.success || !apiResponse.products.length) {
          await this.writeLog('INFO', 'No hay más productos para procesar')
          break
        }

        await this.writeLog('INFO', `Procesando ${apiResponse.products.length} productos de la página ${currentPage}`)

        const batches = this.chunkArray(apiResponse.products, this.batchSize)
        let pageHasErrors = false

        for (let i = 0; i < batches.length && !this.shouldStop; i++) {
          const batch = batches[i]
          const batchStart = performance.now()

          const result = await this.indexProductBatch(batch)

          this.stats.processedProducts += batch.length
          this.stats.indexedProducts += result.indexed
          this.stats.failedProducts += result.failed
          this.stats.batches++
          this.stats.errors.push(...result.errors)

          if (result.failed > 0) {
            pageHasErrors = true
          }

          const batchTime = (performance.now() - batchStart) / 1000
          const speed = (batch.length / batchTime).toFixed(2)

          await this.writeLog('SUCCESS',
            `Batch ${i + 1}/${batches.length} completado: ${result.indexed} indexados, ${result.failed} fallidos en ${batchTime.toFixed(2)}s (${speed} p/s)`
          )

          // Parar en errores si está habilitado
          if (this.stopOnErrors && result.failed > 0) {
            console.log('🛑 Deteniendo por errores (--stop-on-errors habilitado)')
            this.shouldStop = true
            break
          }

          await this.sleep(200)
        }

        // Solo marcar página como exitosa si no hubo errores
        if (!pageHasErrors) {
          this.stats.lastSuccessfulPage = currentPage
        }

        // Guardar checkpoint periódicamente
        if (currentPage % this.checkpointInterval === 0) {
          await this.saveCheckpoint()
        }

        if (apiResponse.pagination) {
          this.stats.totalProducts = apiResponse.pagination.totalItemCount || 0
          hasMorePages = currentPage < (apiResponse.pagination.pageCount || 0)
        } else {
          hasMorePages = false
        }

        currentPage++
        pagesProcessed++
        await this.sleep(100)
      } catch (error) {
        await this.writeLog('ERROR', `Error procesando página ${currentPage}`, { error: error.message })
        this.stats.errors.push({ page: currentPage, error: error.message })
        this.stats.consecutiveErrors++

        currentPage++
        pagesProcessed++
        await this.sleep(2000)
      }
    }

    // Guardar checkpoint final
    await this.saveCheckpoint()

    // Guardar samples de errores finales
    if (this.errorSamples.length > 0) {
      await fs.writeFile(this.errorSamplesFile, JSON.stringify(this.errorSamples, null, 2))
      console.log(`🐛 ${this.errorSamples.length} samples de errores guardados en ${this.errorSamplesFile}`)
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
        successRate: `${((this.stats.indexedProducts / (this.stats.processedProducts || 1)) * 100).toFixed(2)}%`,
        lastSuccessfulPage: this.stats.lastSuccessfulPage,
        consecutiveErrors: this.stats.consecutiveErrors
      },
      errors: this.stats.errors.slice(-100), // Solo últimos 100 errores
      timestamp: new Date().toISOString(),
      files: {
        log: this.logFile,
        checkpoint: this.checkpointFile,
        errorSamples: this.errorSamplesFile
      }
    }

    const reportFile = `logs/typesense-checkpoint-report-${this.timestamp}.json`
    await fs.writeFile(reportFile, JSON.stringify(report, null, 2))

    await this.writeLog('INFO', 'RESUMEN FINAL', report.summary)

    console.log('\n📊 RESUMEN DE INDEXACIÓN TYPESENSE (CON CHECKPOINT):')
    console.log('='.repeat(60))
    console.log(`⏱️  Duración: ${report.summary.duration}`)
    console.log(`📦 Total de productos: ${report.summary.totalProducts}`)
    console.log(`✅ Productos indexados: ${report.summary.indexedProducts}`)
    console.log(`❌ Productos fallidos: ${report.summary.failedProducts}`)
    console.log(`🚀 Velocidad: ${report.summary.speed}`)
    console.log(`📈 Tasa de éxito: ${report.summary.successRate}`)
    console.log(`📄 Última página exitosa: ${report.summary.lastSuccessfulPage}`)
    console.log(`⚠️  Errores consecutivos: ${report.summary.consecutiveErrors}`)
    console.log(`📄 Reporte: ${reportFile}`)
    console.log(`📋 Log: ${this.logFile}`)
    console.log(`💾 Checkpoint: ${this.checkpointFile}`)
    if (this.errorSamples.length > 0) {
      console.log(`🐛 Samples de errores: ${this.errorSamplesFile}`)
    }
    console.log('='.repeat(60))

    console.log('\n🔄 PARA REANUDAR:')
    console.log('node --env-file=.env scripts/typesense-indexer-checkpoint.js --resume')
    console.log('\n🐛 PARA DEBUGEAR ERRORES:')
    console.log('node --env-file=.env scripts/typesense-indexer-checkpoint.js --debug-errors --pages 5')

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
  const indexer = new TypesenseIndexerCheckpoint()

  try {
    await indexer.init()
    await indexer.indexAllProducts()
    await indexer.generateReport()
  } catch (error) {
    console.error('💥 Error fatal:', error.message)
    await indexer.writeLog('ERROR', 'Error fatal', { error: error.message })
    await indexer.saveCheckpoint() // Guardar checkpoint incluso en error fatal
    process.exit(1)
  }
}

// Ejecutar si se llama directamente
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error)
}

export { TypesenseIndexerCheckpoint }
