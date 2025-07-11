// scripts/typesense-indexer-production.js
import axios from 'axios'
import { program } from 'commander'
import { existsSync, readFileSync } from 'fs'
import fs from 'fs/promises'
import { performance } from 'perf_hooks'
import Typesense from 'typesense'

// Configuración CLI
program
  .name('typesense-indexer-production')
  .description('Indexer completo de producción para Typesense con todas las características')
  .version('1.0.0')
  .option('-p, --pages <number>', 'Número máximo de páginas a procesar (0 = todas)', '0')
  .option('-s, --start-page <number>', 'Página inicial', '1')
  .option('-b, --batch-size <number>', 'Tamaño de lote', '50')
  .option('--resume', 'Reanudar desde último checkpoint', false)
  .option('--checkpoint-interval <number>', 'Guardar checkpoint cada N páginas', '25')
  .option('--debug-errors', 'Modo debug detallado para errores', false)
  .option('--stop-on-errors', 'Detener al encontrar errores críticos', false)
  .option('--max-consecutive-errors <number>', 'Máximo errores consecutivos antes de parar', '10')
  .option('--dry-run', 'Simulación sin indexar', false)
  .option('--validate-upsert', 'Validar que el upsert funciona correctamente', false)
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
  pageSize: 100,
  maxRetries: 3,
  retryDelay: 1000,
  timeout: 30000
}

class TypesenseIndexerProduction {
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
      consecutiveErrors: 0,
      upsertValidated: false
    }

    this.batchSize = parseInt(options.batchSize) || 50
    this.maxPages = parseInt(options.pages) || 0
    this.startPage = parseInt(options.startPage) || 1
    this.checkpointInterval = parseInt(options.checkpointInterval) || 25
    this.debugErrors = options.debugErrors || false
    this.stopOnErrors = options.stopOnErrors || false
    this.maxConsecutiveErrors = parseInt(options.maxConsecutiveErrors) || 10
    this.dryRun = options.dryRun || false
    this.validateUpsert = options.validateUpsert || false

    this.timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, '-')
    this.logFile = `logs/typesense-production-${this.timestamp}.log`
    this.checkpointFile = `logs/checkpoint-production-${this.collectionName}.json`
    this.errorSamplesFile = `logs/error-samples-production-${this.timestamp}.json`
    this.reportFile = `logs/production-report-${this.timestamp}.json`

    this.shouldStop = false
    this.errorSamples = []
    this.performanceMetrics = {
      apiCallTimes: [],
      transformTimes: [],
      indexTimes: [],
      totalBatches: 0
    }
  }

  async init () {
    console.log('🚀 TYPESENSE INDEXER PRODUCTION v1.0')
    console.log('='.repeat(50))
    console.log('🎯 Características incluidas:')
    console.log('  ✅ Campo "id" como clave primaria (sin duplicados)')
    console.log('  ✅ Checkpoint automático y manual')
    console.log('  ✅ Manejo elegante de señales (Ctrl+C)')
    console.log('  ✅ Control de errores consecutivos')
    console.log('  ✅ Logging detallado y samples de errores')
    console.log('  ✅ Métricas de rendimiento')
    console.log('  ✅ Warranties y campos complejos corregidos')
    console.log('  ✅ Modo dry-run y validación de upsert')
    console.log('='.repeat(50))

    // Manejar señales para parada elegante
    this.setupSignalHandlers()

    // Resumir desde checkpoint si se solicita
    if (options.resume) {
      await this.loadCheckpoint()
    }

    console.log('\n📋 CONFIGURACIÓN ACTUAL:')
    console.log(`  📄 Páginas: ${this.maxPages || 'TODAS (⚠️  indexación completa)'}`)
    console.log(`  🔢 Desde página: ${this.startPage}`)
    console.log(`  📦 Batch size: ${this.batchSize}`)
    console.log(`  💾 Checkpoint cada: ${this.checkpointInterval} páginas`)
    console.log(`  🐛 Debug errores: ${this.debugErrors ? '✅' : '❌'}`)
    console.log(`  🛑 Parar en errores: ${this.stopOnErrors ? '✅' : '❌'}`)
    console.log(`  🔄 Modo dry-run: ${this.dryRun ? '✅ (sin indexar)' : '❌'}`)
    console.log(`  🧪 Validar upsert: ${this.validateUpsert ? '✅' : '❌'}`)
    console.log(`  ⚠️  Max errores consecutivos: ${this.maxConsecutiveErrors}`)

    this.stats.startTime = performance.now()

    await this.ensureLogDirectory()
    await this.writeLog('INFO', '🚀 Indexer Production iniciado', {
      configuration: {
        maxPages: this.maxPages,
        startPage: this.startPage,
        batchSize: this.batchSize,
        checkpointInterval: this.checkpointInterval,
        debugErrors: this.debugErrors,
        stopOnErrors: this.stopOnErrors,
        dryRun: this.dryRun,
        validateUpsert: this.validateUpsert
      }
    })

    try {
      await this.checkConnection()
      await this.validateExistingSchema()

      if (this.validateUpsert) {
        await this.validateUpsertFunctionality()
      }

      console.log('✅ Configuración validada - Listo para indexar')
    } catch (error) {
      await this.writeLog('ERROR', `Error en inicialización: ${error.message}`)
      throw error
    }
  }

  setupSignalHandlers () {
    // Manejar Ctrl+C elegantemente
    process.on('SIGINT', async () => {
      console.log('\n🛑 SEÑAL DE INTERRUPCIÓN RECIBIDA (Ctrl+C)')
      console.log('💾 Guardando checkpoint y estadísticas...')
      this.shouldStop = true
      await this.saveCheckpoint()
      await this.generateReport(true) // true = interrupted
      await this.writeLog('INFO', 'Proceso interrumpido elegantemente por usuario')
      console.log('\n✅ Checkpoint guardado. Puedes reanudar con --resume')
      process.exit(0)
    })

    // Manejar SIGTERM
    process.on('SIGTERM', async () => {
      console.log('\n🛑 SEÑAL SIGTERM RECIBIDA')
      this.shouldStop = true
      await this.saveCheckpoint()
      await this.generateReport(true)
      await this.writeLog('INFO', 'Proceso terminado por SIGTERM')
      process.exit(0)
    })
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

  async loadCheckpoint () {
    try {
      if (existsSync(this.checkpointFile)) {
        const checkpoint = JSON.parse(readFileSync(this.checkpointFile, 'utf8'))

        this.startPage = checkpoint.lastSuccessfulPage + 1
        this.stats = { ...this.stats, ...checkpoint.stats }

        console.log('\n📂 CHECKPOINT CARGADO:')
        console.log(`  🔄 Reanudando desde página: ${this.startPage}`)
        console.log(`  📊 Productos indexados previamente: ${checkpoint.stats.indexedProducts.toLocaleString()}`)
        console.log(`  ❌ Productos fallidos previamente: ${checkpoint.stats.failedProducts.toLocaleString()}`)
        console.log(`  ⏱️  Última ejecución: ${new Date(checkpoint.timestamp).toLocaleString()}`)

        await this.writeLog('INFO', 'Checkpoint cargado exitosamente', {
          resumeFromPage: this.startPage,
          previousStats: checkpoint.stats
        })
      } else {
        console.log('📂 No se encontró checkpoint anterior - comenzando desde el inicio')
      }
    } catch (error) {
      console.log(`⚠️ Error cargando checkpoint: ${error.message}`)
      await this.writeLog('WARN', 'Error cargando checkpoint, comenzando desde configuración inicial')
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
        },
        performanceMetrics: this.performanceMetrics
      }

      await fs.writeFile(this.checkpointFile, JSON.stringify(checkpoint, null, 2))
      await this.writeLog('INFO', `💾 Checkpoint guardado: página ${this.stats.lastSuccessfulPage}`)
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

  async checkConnection () {
    try {
      const health = await this.client.health.retrieve()
      if (health.ok !== true) {
        throw new Error('Typesense no está disponible')
      }
      await this.writeLog('SUCCESS', 'Conexión con Typesense establecida')
    } catch (error) {
      throw new Error(`Error conectando con Typesense: ${error.message}`)
    }
  }

  async validateExistingSchema () {
    try {
      const collection = await this.client.collections(this.collectionName).retrieve()

      await this.writeLog('INFO', `Esquema validado: ${collection.fields?.length || 0} campos`, {
        totalFields: collection.fields?.length,
        currentDocuments: collection.num_documents,
        defaultSortingField: collection.default_sorting_field
      })

      console.log(`📋 Esquema de la colección '${this.collectionName}':`)
      console.log(`  📦 Documentos actuales: ${collection.num_documents.toLocaleString()}`)
      console.log(`  📋 Total campos: ${collection.fields?.length || 0}`)
      console.log(`  🎯 Campo de ordenamiento: ${collection.default_sorting_field}`)

      return collection
    } catch (error) {
      throw new Error(`Error validando esquema: ${error.message}`)
    }
  }

  async validateUpsertFunctionality () {
    console.log('\n🧪 VALIDANDO FUNCIONALIDAD DE UPSERT...')

    try {
      // Crear un documento de prueba único
      const testDoc = {
        id: `test-upsert-${Date.now()}`,
        objectID: `test-upsert-${Date.now()}`,
        product_id: 999999999,
        external_id: 'test-999999999',
        title: 'Producto de Prueba Upsert',
        title_seo: 'producto-de-prueba-upsert',
        stock: 1,
        is_active: true,
        sale_price: 100.00,
        indexing_date: Math.floor(Date.now() / 1000),
        relevance_score: 50.0
      }

      // Primera inserción
      console.log('📤 Insertando documento de prueba...')
      const firstResult = await this.client.collections(this.collectionName).documents().import([testDoc], {
        action: 'upsert'
      })

      if (firstResult[0].success !== true) {
        throw new Error(`Error en primera inserción: ${firstResult[0].error}`)
      }

      // Verificar que se insertó
      await this.sleep(500)
      const searchAfterFirst = await this.client.collections(this.collectionName).documents().search({
        q: '*',
        filter_by: `id:=${testDoc.id}`
      })

      if (searchAfterFirst.found !== 1) {
        throw new Error(`Documento no se insertó correctamente. Encontrados: ${searchAfterFirst.found}`)
      }

      console.log('✅ Primera inserción exitosa')

      // Segunda inserción (mismo documento, debería hacer upsert)
      testDoc.title = 'Producto de Prueba Upsert ACTUALIZADO'
      testDoc.indexing_date = Math.floor(Date.now() / 1000)

      console.log('🔄 Probando upsert (segunda inserción del mismo documento)...')
      const secondResult = await this.client.collections(this.collectionName).documents().import([testDoc], {
        action: 'upsert'
      })

      if (secondResult[0].success !== true) {
        throw new Error(`Error en upsert: ${secondResult[0].error}`)
      }

      // Verificar que sigue siendo 1 documento (no duplicado)
      await this.sleep(500)
      const searchAfterSecond = await this.client.collections(this.collectionName).documents().search({
        q: '*',
        filter_by: `id:=${testDoc.id}`
      })

      if (searchAfterSecond.found !== 1) {
        throw new Error(`UPSERT FALLÓ: Se encontraron ${searchAfterSecond.found} documentos, debería ser 1`)
      }

      // Verificar que el documento se actualizó
      const updatedDoc = searchAfterSecond.hits[0].document
      if (!updatedDoc.title.includes('ACTUALIZADO')) {
        throw new Error('El documento no se actualizó correctamente')
      }

      console.log('✅ Upsert funcionando correctamente')

      // Limpiar documento de prueba
      await this.client.collections(this.collectionName).documents(testDoc.id).delete()
      console.log('🧹 Documento de prueba eliminado')

      this.stats.upsertValidated = true
      await this.writeLog('SUCCESS', 'Validación de upsert completada exitosamente')
    } catch (error) {
      console.log(`❌ Error en validación de upsert: ${error.message}`)
      await this.writeLog('ERROR', 'Error en validación de upsert', { error: error.message })

      if (this.stopOnErrors) {
        throw new Error('Validación de upsert falló y --stop-on-errors está habilitado')
      }
    }
  }

  async fetchProductsFromAPI (page = 1) {
    const url = `${API_CONFIG.baseUrl}?page_size=${API_CONFIG.pageSize}&page=${page}`
    const apiStart = performance.now()

    for (let attempt = 1; attempt <= API_CONFIG.maxRetries; attempt++) {
      try {
        await this.writeLog('INFO', `Obteniendo página ${page} (intento ${attempt})`)

        const response = await axios.get(url, {
          timeout: API_CONFIG.timeout,
          headers: {
            'User-Agent': 'TypesenseProductionIndexer/1.0',
            Accept: 'application/json'
          }
        })

        if (response.data && response.data.metadata && response.data.metadata.is_error === false) {
          const apiTime = performance.now() - apiStart
          this.performanceMetrics.apiCallTimes.push(apiTime)

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

  // 🔧 FUNCIONES DE TRANSFORMACIÓN CON TODOS LOS FIXES

  fixWarranties (warranties) {
    if (!warranties) return null
    if (typeof warranties === 'object' && !Array.isArray(warranties)) return warranties
    if (Array.isArray(warranties) && warranties.length > 0) {
      const firstWarranty = warranties[0]
      if (typeof firstWarranty === 'object') return firstWarranty
    }
    return null
  }

  cleanArrayFields (data) {
    const cleaned = { ...data }

    if (Array.isArray(cleaned.attributes)) {
      cleaned.attributes = cleaned.attributes.filter(attr =>
        attr && typeof attr === 'object' && attr.name && attr.value
      )
    }

    if (Array.isArray(cleaned.pictures)) {
      cleaned.pictures = cleaned.pictures.filter(pic =>
        pic && typeof pic === 'object' && pic.source
      )
    }

    if (Array.isArray(cleaned.categories)) {
      cleaned.categories = cleaned.categories.filter(cat =>
        cat && Array.isArray(cat) && cat.length > 0
      )
    }

    return cleaned
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
    try {
      let score = 50.0

      const stock = product.stock || 0
      if (stock > 0) score += Math.min(stock * 0.3, 10)

      const avgRating = product.rating?.average_score || 0
      if (avgRating > 0) score += avgRating * 3

      const totalReviews = product.rating?.total_reviews || 0
      if (totalReviews > 0) score += Math.min(Math.log10(totalReviews + 1) * 3, 10)

      const discount = product.pricing?.percentage_discount || 0
      if (discount > 0) score += Math.min(discount * 0.16, 8)

      if (product.features?.super_express === true) score += 10
      if (product.shipping?.is_free === true) score += 7
      if (product.is_active === true) score += 5

      const apiRelevanceSales = product.relevance_sales || 0
      const apiRelevanceAmount = product.relevance_amount || 0

      if (apiRelevanceSales > 0) score += Math.min(apiRelevanceSales * 0.15, 12)
      if (apiRelevanceAmount > 0) score += Math.min(apiRelevanceAmount * 0.08, 8)

      return Math.min(Math.round(score * 100) / 100, 100.00)
    } catch (error) {
      return 50.0
    }
  }

  transformProductForTypesense (product) {
    const transformStart = performance.now()

    try {
      const categories = this.parseCategories(product.categories)
      const relevanceScore = this.calculateRelevanceScore(product)
      const fixedWarranties = this.fixWarranties(product.warranties)

      // 🎯 DOCUMENTO COMPLETO CON CAMPO "id" COMO CLAVE PRIMARIA
      const document = {
        // ⭐ CLAVE PRIMARIA REAL DE TYPESENSE
        id: String(product.id || product.external_id),

        // Campos obligatorios del esquema
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

        // Campos opcionales con validación completa
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

        // Objetos con validación robusta
        seller: product.seller || null,
        pricing: product.pricing && typeof product.pricing === 'object' ? product.pricing : {},
        shipping: product.shipping && typeof product.shipping === 'object' ? product.shipping : {},
        rating: product.rating && typeof product.rating === 'object' ? product.rating : {},
        features: product.features && typeof product.features === 'object' ? product.features : {},

        // 🔧 WARRANTIES CORREGIDO - Como objeto, no array
        warranties: fixedWarranties,

        // Arrays limpios y validados
        pictures: Array.isArray(product.pictures) ? product.pictures : [],
        attributes: Array.isArray(product.attributes) ? product.attributes : [],
        categories: Array.isArray(product.categories) ? product.categories : [],
        videos: Array.isArray(product.videos) ? product.videos : [],
        volumetries: Array.isArray(product.volumetries) ? product.volumetries : [],

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

        // Campos calculados
        price: product.pricing?.list_price || 0,
        percent_off: product.pricing?.percentage_discount || 0,
        fulfillment: Boolean(product.features?.super_express || product.features?.fulfillment_id),
        has_free_shipping: Boolean(product.shipping?.is_free || product.shipping?.free_shipping),
        store_only: Boolean(product.is_store_only || product.features?.is_store_only),
        store_pickup: Boolean(product.is_store_pickup || product.features?.is_store_pickup),

        // Fotos (alias de pictures)
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

        // Sellers como array
        sellers: product.seller ? [product.seller] : [],

        // Campos de rating
        review_rating: product.rating?.average_score || product.rating?.average || 0,
        total_reviews: product.rating?.total_reviews || product.rating?.count || 0,
        store_rating: product.seller?.store_rating || 0,

        // Campos adicionales del esquema
        ccs_months: Array.isArray(product.ccs_months) ? product.ccs_months : [],
        fecha_alta_cms: product.fecha_alta_cms || Math.floor(Date.now() / 1000),
        temporada: product.temporada || 0,
        variations: product.variations && typeof product.variations === 'object' ? product.variations : {}
      }

      // Limpiar campos problemáticos
      const cleanedDocument = this.cleanArrayFields(document)

      const transformTime = performance.now() - transformStart
      this.performanceMetrics.transformTimes.push(transformTime)

      return cleanedDocument
    } catch (error) {
      const transformTime = performance.now() - transformStart
      this.performanceMetrics.transformTimes.push(transformTime)
      throw new Error(`Error transformando producto ${product.id}: ${error.message}`)
    }
  }

  async indexProductBatch (products) {
    if (!products || products.length === 0) {
      return { indexed: 0, failed: 0, errors: [] }
    }

    const indexStart = performance.now()

    try {
      const documents = []
      const transformErrors = []

      // Transformar productos
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

          if (this.debugErrors) {
            await this.writeLog('ERROR', `Error transformando producto ${product.id}`, { error: error.message })
          }
        }
      }

      if (documents.length === 0) {
        return { indexed: 0, failed: products.length, errors: transformErrors }
      }

      await this.writeLog('INFO', `Indexando batch de ${documents.length} documentos`)

      // Modo dry-run
      if (this.dryRun) {
        await this.writeLog('INFO', `[DRY-RUN] Se indexarían ${documents.length} documentos`)
        return {
          indexed: documents.length,
          failed: transformErrors.length,
          errors: transformErrors
        }
      }

      // Indexar en Typesense usando el campo "id" como clave primaria
      const results = await this.client.collections(this.collectionName).documents().import(documents, {
        action: 'upsert'
      })

      // Procesar resultados
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

      const indexTime = performance.now() - indexStart
      this.performanceMetrics.indexTimes.push(indexTime)
      this.performanceMetrics.totalBatches++

      await this.writeLog('SUCCESS', `Batch indexado: ${indexed} exitosos, ${failed} fallidos`)

      return {
        indexed,
        failed: failed + transformErrors.length,
        errors: [...transformErrors, ...indexErrors]
      }
    } catch (error) {
      const indexTime = performance.now() - indexStart
      this.performanceMetrics.indexTimes.push(indexTime)
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

    console.log('\n🚀 INICIANDO INDEXACIÓN DE PRODUCCIÓN...')
    await this.writeLog('INFO', 'Iniciando indexación de producción completa')

    while (hasMorePages && !this.shouldStop) {
      // Verificar límite de páginas
      if (this.maxPages > 0 && pagesProcessed >= this.maxPages) {
        console.log(`🛑 Límite de páginas alcanzado (${this.maxPages} páginas)`)
        await this.writeLog('INFO', `Límite de páginas alcanzado: ${this.maxPages}`)
        break
      }

      // Verificar errores consecutivos
      if (this.stats.consecutiveErrors >= this.maxConsecutiveErrors) {
        console.log(`🛑 Demasiados errores consecutivos (${this.stats.consecutiveErrors}). Deteniendo...`)
        await this.writeLog('ERROR', `Proceso detenido por ${this.stats.consecutiveErrors} errores consecutivos`)
        break
      }

      try {
        console.log(`\n📄 PROCESANDO PÁGINA ${currentPage}:`)

        const apiResponse = await this.fetchProductsFromAPI(currentPage)

        if (!apiResponse.success || !apiResponse.products.length) {
          await this.writeLog('INFO', 'No hay más productos para procesar')
          console.log('✅ No hay más productos - indexación completada')
          break
        }

        await this.writeLog('INFO', `Procesando ${apiResponse.products.length} productos de la página ${currentPage}`)

        // Procesar en batches
        const batches = this.chunkArray(apiResponse.products, this.batchSize)
        let pageHasErrors = false

        for (let i = 0; i < batches.length && !this.shouldStop; i++) {
          const batch = batches[i]
          const batchStart = performance.now()

          console.log(`  📦 Batch ${i + 1}/${batches.length}: ${batch.length} productos`)

          const result = await this.indexProductBatch(batch)

          // Actualizar estadísticas
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

          console.log(`    ✅ ${result.indexed} indexados, ❌ ${result.failed} fallidos en ${batchTime.toFixed(2)}s (${speed} p/s)`)

          await this.writeLog('SUCCESS',
            `Batch ${i + 1}/${batches.length} completado: ${result.indexed} indexados, ${result.failed} fallidos en ${batchTime.toFixed(2)}s (${speed} p/s)`
          )

          // Parar en errores críticos si está habilitado
          if (this.stopOnErrors && result.failed > 0) {
            console.log('🛑 Deteniendo por errores (--stop-on-errors habilitado)')
            this.shouldStop = true
            break
          }

          // Pausa entre batches para no sobrecargar
          await this.sleep(200)
        }

        // Solo marcar página como exitosa si no hubo errores críticos
        if (!pageHasErrors || !this.stopOnErrors) {
          this.stats.lastSuccessfulPage = currentPage
        }

        // Guardar checkpoint periódicamente
        if (currentPage % this.checkpointInterval === 0) {
          console.log(`💾 Guardando checkpoint automático (página ${currentPage})...`)
          await this.saveCheckpoint()
        }

        // Mostrar estadísticas de progreso
        if (currentPage % 10 === 0) {
          await this.showProgressStats()
        }

        // Verificar paginación
        if (apiResponse.pagination) {
          this.stats.totalProducts = apiResponse.pagination.totalItemCount || 0
          hasMorePages = currentPage < (apiResponse.pagination.pageCount || 0)

          if (currentPage % 50 === 0) {
            const progress = ((currentPage / (apiResponse.pagination.pageCount || 1)) * 100).toFixed(1)
            console.log(`📊 Progreso general: ${progress}% (${currentPage}/${apiResponse.pagination.pageCount} páginas)`)
          }
        } else {
          hasMorePages = false
        }

        currentPage++
        pagesProcessed++

        // Pausa entre páginas
        await this.sleep(100)
      } catch (error) {
        console.log(`💥 ERROR EN PÁGINA ${currentPage}: ${error.message}`)
        await this.writeLog('ERROR', `Error procesando página ${currentPage}`, { error: error.message })
        this.stats.errors.push({ page: currentPage, error: error.message })
        this.stats.consecutiveErrors++

        currentPage++
        pagesProcessed++

        // Pausa más larga en caso de error
        await this.sleep(2000)
      }
    }

    // Guardar checkpoint final
    await this.saveCheckpoint()

    // Guardar samples de errores finales
    if (this.errorSamples.length > 0) {
      await fs.writeFile(this.errorSamplesFile, JSON.stringify(this.errorSamples, null, 2))
      console.log(`🐛 ${this.errorSamples.length} samples de errores guardados`)
    }

    console.log('\n🎉 INDEXACIÓN COMPLETADA')
  }

  async showProgressStats () {
    const duration = (performance.now() - this.stats.startTime) / 1000
    const speed = this.stats.processedProducts / duration
    const successRate = (this.stats.indexedProducts / (this.stats.processedProducts || 1)) * 100

    console.log('\n📊 ESTADÍSTICAS DE PROGRESO:')
    console.log(`  ⏱️  Tiempo transcurrido: ${this.formatDuration(duration)}`)
    console.log(`  📦 Productos procesados: ${this.stats.processedProducts.toLocaleString()}`)
    console.log(`  ✅ Productos indexados: ${this.stats.indexedProducts.toLocaleString()}`)
    console.log(`  ❌ Productos fallidos: ${this.stats.failedProducts.toLocaleString()}`)
    console.log(`  🚀 Velocidad promedio: ${speed.toFixed(2)} productos/seg`)
    console.log(`  📈 Tasa de éxito: ${successRate.toFixed(1)}%`)
    console.log(`  📄 Última página exitosa: ${this.stats.lastSuccessfulPage}`)
  }

  formatDuration (seconds) {
    const hours = Math.floor(seconds / 3600)
    const minutes = Math.floor((seconds % 3600) / 60)
    const secs = Math.floor(seconds % 60)

    if (hours > 0) {
      return `${hours}h ${minutes}m ${secs}s`
    } else if (minutes > 0) {
      return `${minutes}m ${secs}s`
    } else {
      return `${secs}s`
    }
  }

  async generateReport (interrupted = false) {
    this.stats.endTime = performance.now()
    const duration = (this.stats.endTime - this.stats.startTime) / 1000

    // Calcular métricas de rendimiento
    const avgApiTime = this.performanceMetrics.apiCallTimes.length > 0
      ? this.performanceMetrics.apiCallTimes.reduce((a, b) => a + b, 0) / this.performanceMetrics.apiCallTimes.length
      : 0

    const avgTransformTime = this.performanceMetrics.transformTimes.length > 0
      ? this.performanceMetrics.transformTimes.reduce((a, b) => a + b, 0) / this.performanceMetrics.transformTimes.length
      : 0

    const avgIndexTime = this.performanceMetrics.indexTimes.length > 0
      ? this.performanceMetrics.indexTimes.reduce((a, b) => a + b, 0) / this.performanceMetrics.indexTimes.length
      : 0

    const report = {
      summary: {
        status: interrupted ? 'INTERRUPTED' : 'COMPLETED',
        duration: `${duration.toFixed(2)} segundos`,
        formattedDuration: this.formatDuration(duration),
        totalProducts: this.stats.totalProducts,
        processedProducts: this.stats.processedProducts,
        indexedProducts: this.stats.indexedProducts,
        failedProducts: this.stats.failedProducts,
        batches: this.stats.batches,
        speed: `${(this.stats.processedProducts / duration).toFixed(2)} productos/seg`,
        successRate: `${((this.stats.indexedProducts / (this.stats.processedProducts || 1)) * 100).toFixed(2)}%`,
        lastSuccessfulPage: this.stats.lastSuccessfulPage,
        consecutiveErrors: this.stats.consecutiveErrors,
        upsertValidated: this.stats.upsertValidated,
        fixes: ['ID_FIELD_PRIMARY_KEY', 'WARRANTIES_CORRECTED', 'ARRAY_VALIDATION', 'ERROR_HANDLING']
      },
      performance: {
        avgApiCallTime: `${avgApiTime.toFixed(2)}ms`,
        avgTransformTime: `${avgTransformTime.toFixed(2)}ms`,
        avgIndexTime: `${avgIndexTime.toFixed(2)}ms`,
        totalBatches: this.performanceMetrics.totalBatches,
        apiCalls: this.performanceMetrics.apiCallTimes.length,
        transforms: this.performanceMetrics.transformTimes.length,
        indexes: this.performanceMetrics.indexTimes.length
      },
      configuration: {
        maxPages: this.maxPages,
        startPage: this.startPage,
        batchSize: this.batchSize,
        checkpointInterval: this.checkpointInterval,
        debugErrors: this.debugErrors,
        stopOnErrors: this.stopOnErrors,
        dryRun: this.dryRun,
        validateUpsert: this.validateUpsert
      },
      errors: {
        totalErrors: this.stats.errors.length,
        lastErrors: this.stats.errors.slice(-20), // Últimos 20 errores
        errorSamplesCount: this.errorSamples.length
      },
      files: {
        log: this.logFile,
        checkpoint: this.checkpointFile,
        errorSamples: this.errorSamplesFile,
        report: this.reportFile
      },
      timestamp: new Date().toISOString()
    }

    // Guardar reporte detallado
    await fs.writeFile(this.reportFile, JSON.stringify(report, null, 2))

    // Log del resumen
    await this.writeLog('INFO', 'REPORTE FINAL GENERADO', report.summary)

    // Mostrar resumen en consola
    console.log('\n📊 REPORTE FINAL DE INDEXACIÓN TYPESENSE PRODUCTION:')
    console.log('='.repeat(70))
    console.log(`🏁 Estado: ${report.summary.status}`)
    console.log(`⏱️  Duración: ${report.summary.formattedDuration}`)
    console.log(`📦 Total API: ${report.summary.totalProducts.toLocaleString()}`)
    console.log(`📊 Procesados: ${report.summary.processedProducts.toLocaleString()}`)
    console.log(`✅ Indexados: ${report.summary.indexedProducts.toLocaleString()}`)
    console.log(`❌ Fallidos: ${report.summary.failedProducts.toLocaleString()}`)
    console.log(`📈 Tasa éxito: ${report.summary.successRate}`)
    console.log(`🚀 Velocidad: ${report.summary.speed}`)
    console.log(`📄 Última página: ${report.summary.lastSuccessfulPage}`)
    console.log(`🧪 Upsert validado: ${report.summary.upsertValidated ? '✅' : '❌'}`)

    if (report.performance.totalBatches > 0) {
      console.log('\n⚡ MÉTRICAS DE RENDIMIENTO:')
      console.log(`  📡 API promedio: ${report.performance.avgApiCallTime}`)
      console.log(`  🔧 Transform promedio: ${report.performance.avgTransformTime}`)
      console.log(`  📤 Index promedio: ${report.performance.avgIndexTime}`)
      console.log(`  📊 Total batches: ${report.performance.totalBatches}`)
    }

    console.log('\n🔧 FIXES APLICADOS:')
    report.summary.fixes.forEach(fix => {
      console.log(`  ✅ ${fix}`)
    })

    if (report.errors.totalErrors > 0) {
      console.log(`\n⚠️  ERRORES: ${report.errors.totalErrors} errores registrados`)
      console.log(`🐛 Samples guardados: ${report.errors.errorSamplesCount}`)
    }

    console.log('\n📁 ARCHIVOS GENERADOS:')
    console.log(`📋 Log: ${report.files.log}`)
    console.log(`📊 Reporte: ${report.files.report}`)
    console.log(`💾 Checkpoint: ${report.files.checkpoint}`)
    if (report.errors.errorSamplesCount > 0) {
      console.log(`🐛 Error samples: ${report.files.errorSamples}`)
    }

    if (interrupted) {
      console.log('\n🔄 PARA REANUDAR:')
      console.log('node --env-file=.env scripts/typesense-indexer-production.js --resume')
    } else {
      console.log('\n🎉 INDEXACIÓN COMPLETADA EXITOSAMENTE')
    }

    console.log('='.repeat(70))

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
  const indexer = new TypesenseIndexerProduction()

  try {
    await indexer.init()
    await indexer.indexAllProducts()
    await indexer.generateReport()
  } catch (error) {
    console.error('💥 Error fatal:', error.message)
    await indexer.writeLog('ERROR', 'Error fatal', { error: error.message })
    await indexer.saveCheckpoint() // Guardar checkpoint incluso en error fatal
    await indexer.generateReport(true) // true = interrupted by error
    process.exit(1)
  }
}

// Ejecutar si se llama directamente
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error)
}

export { TypesenseIndexerProduction }
