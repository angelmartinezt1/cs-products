// scripts/typesense-indexer-detailed-debug.js
import axios from 'axios'
import { program } from 'commander'
import fs from 'fs/promises'
import { performance } from 'perf_hooks'
import Typesense from 'typesense'

// Configuración CLI
program
  .option('-p, --pages <number>', 'Número máximo de páginas a procesar', '5')
  .option('-s, --start-page <number>', 'Página inicial', '1')
  .option('-b, --batch-size <number>', 'Tamaño de lote', '10')
  .option('--single-page <number>', 'Procesar solo una página específica')
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

class TypesenseDetailedDebugger {
  constructor () {
    this.client = new Typesense.Client(TYPESENSE_CONFIG)
    this.collectionName = TYPESENSE_CONFIG.collectionName

    this.stats = {
      totalProducts: 0,
      processedProducts: 0,
      indexedProducts: 0,
      failedProducts: 0,
      batches: 0,
      specificErrors: {},
      startTime: null,
      endTime: null
    }

    this.batchSize = parseInt(options.batchSize) || 10  // Más pequeño para debug
    this.maxPages = parseInt(options.pages) || 5
    this.startPage = parseInt(options.startPage) || 1
    this.singlePage = options.singlePage ? parseInt(options.singlePage) : null

    this.timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, '-')
    this.logFile = `logs/detailed-debug-${this.timestamp}.log`
    this.errorDetailsFile = `logs/error-details-${this.timestamp}.json`
    this.documentsFile = `logs/documents-sample-${this.timestamp}.json`

    this.allErrorDetails = []
    this.documentSamples = []
  }

  async init () {
    console.log('🔍 Iniciando Debug Detallado de Typesense...')
    console.log(`📋 Configuración: páginas=${this.maxPages}, desde=${this.startPage}, batch=${this.batchSize}`)

    if (this.singlePage) {
      console.log(`🎯 Modo página única: ${this.singlePage}`)
      this.startPage = this.singlePage
      this.maxPages = 1
    }

    this.stats.startTime = performance.now()

    await this.ensureLogDirectory()
    await this.writeLog('INFO', 'Debug detallado iniciado')

    try {
      await this.checkConnection()
      await this.getSchemaDetails()
      console.log('✅ Configuración validada')
    } catch (error) {
      await this.writeLog('ERROR', `Error en inicialización: ${error.message}`)
      throw error
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

  async getSchemaDetails () {
    try {
      const collection = await this.client.collections(this.collectionName).retrieve()

      const requiredFields = collection.fields.filter(f => !f.optional)
      const optionalFields = collection.fields.filter(f => f.optional)

      console.log(`📋 Esquema de la colección '${this.collectionName}':`)
      console.log(`  📦 Total campos: ${collection.fields.length}`)
      console.log(`  ✅ Campos obligatorios: ${requiredFields.length}`)
      console.log(`  ⚪ Campos opcionales: ${optionalFields.length}`)

      await this.writeLog('INFO', 'Esquema obtenido', {
        totalFields: collection.fields.length,
        requiredFields: requiredFields.map(f => ({ name: f.name, type: f.type })),
        optionalFields: optionalFields.length
      })

      // Mostrar campos obligatorios
      console.log('\n📋 CAMPOS OBLIGATORIOS:')
      requiredFields.forEach(field => {
        console.log(`  - ${field.name} (${field.type})`)
      })

      return collection
    } catch (error) {
      throw new Error(`Error obteniendo esquema: ${error.message}`)
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
            'User-Agent': 'TypesenseDetailedDebugger/1.0',
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

      // Documento básico pero completo
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
        relevance_score: relevanceScore
      }

      // Agregar campos opcionales gradualmente para identificar problemas
      const optionalFields = {
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

        // Objetos simples primero
        seller: product.seller || null,
        pricing: product.pricing && typeof product.pricing === 'object' ? product.pricing : {},
        shipping: product.shipping && typeof product.shipping === 'object' ? product.shipping : {},
        rating: product.rating && typeof product.rating === 'object' ? product.rating : {},
        features: product.features && typeof product.features === 'object' ? product.features : {},

        // Arrays
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
        warranties: product.warranties && typeof product.warranties === 'object' ? product.warranties : {},

        // Campos calculados
        price: product.pricing?.list_price || 0,
        percent_off: product.pricing?.percentage_discount || 0,
        fulfillment: Boolean(product.features?.super_express || product.features?.fulfillment_id),
        has_free_shipping: Boolean(product.shipping?.is_free || product.shipping?.free_shipping),
        store_only: Boolean(product.is_store_only || product.features?.is_store_only),
        store_pickup: Boolean(product.is_store_pickup || product.features?.is_store_pickup),

        // Fotos (alias)
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

        // Campos adicionales
        ccs_months: Array.isArray(product.ccs_months) ? product.ccs_months : [],
        fecha_alta_cms: product.fecha_alta_cms || Math.floor(Date.now() / 1000),
        temporada: product.temporada || 0,
        variations: product.variations && typeof product.variations === 'object' ? product.variations : {}
      }

      // Combinar documento base con campos opcionales
      Object.assign(document, optionalFields)

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
    try {
      let score = 50.0 // Score base

      const stock = product.stock || 0
      if (stock > 0) {
        score += Math.min(stock * 0.1, 5)
      }

      const avgRating = product.rating?.average_score || 0
      if (avgRating > 0) {
        score += avgRating * 2
      }

      if (product.is_active === true) score += 5
      if (product.features?.super_express === true) score += 10
      if (product.shipping?.is_free === true) score += 5

      return Math.min(Math.round(score * 100) / 100, 100.00)
    } catch (error) {
      return 50.0
    }
  }

  async indexProductBatchDetailed (products) {
    if (!products || products.length === 0) {
      return { indexed: 0, failed: 0, errors: [], details: [] }
    }

    console.log(`\n🔍 ANALIZANDO BATCH DE ${products.length} PRODUCTOS:`)

    try {
      // Transformar productos
      const documents = []
      const transformErrors = []

      for (let i = 0; i < products.length; i++) {
        const product = products[i]
        try {
          const doc = this.transformProductForTypesense(product)
          documents.push(doc)

          // Guardar sample del documento para análisis
          if (this.documentSamples.length < 5) {
            this.documentSamples.push({
              productId: product.id,
              title: product.title,
              transformedDocument: doc
            })
          }

          console.log(`  ✅ ${i + 1}. Producto ${product.id} transformado: ${product.title?.substring(0, 50)}...`)
        } catch (error) {
          transformErrors.push({
            productId: product.id,
            error: error.message,
            originalProduct: product
          })
          console.log(`  ❌ ${i + 1}. Producto ${product.id} falló en transformación: ${error.message}`)
        }
      }

      if (documents.length === 0) {
        console.log('❌ No hay documentos para indexar después de transformación')
        return { indexed: 0, failed: products.length, errors: transformErrors, details: [] }
      }

      console.log(`\n📤 ENVIANDO ${documents.length} DOCUMENTOS A TYPESENSE...`)

      // Indexar en Typesense
      const results = await this.client.collections(this.collectionName).documents().import(documents, {
        action: 'upsert'
      })

      console.log(`\n📥 RESPUESTA DE TYPESENSE RECIBIDA: ${results.length} resultados`)

      // Análisis detallado de resultados
      let indexed = 0
      let failed = 0
      const indexErrors = []
      const successfulDocs = []
      const failedDocs = []

      for (let i = 0; i < results.length; i++) {
        const result = results[i]
        const originalDoc = documents[i]

        if (result.success === true) {
          indexed++
          successfulDocs.push({
            productId: originalDoc.product_id,
            title: originalDoc.title,
            objectID: originalDoc.objectID
          })
          console.log(`  ✅ ${i + 1}. ${originalDoc.title?.substring(0, 50)}... → ÉXITO`)
        } else {
          failed++

          const errorDetail = {
            productId: originalDoc.product_id,
            title: originalDoc.title,
            objectID: originalDoc.objectID,
            error: result.error || 'Unknown error',
            errorCode: result.code,
            document: result.document || null,
            originalDocument: originalDoc
          }

          failedDocs.push(errorDetail)
          indexErrors.push(errorDetail)

          console.log(`  ❌ ${i + 1}. ${originalDoc.title?.substring(0, 50)}... → ERROR: ${result.error}`)

          // Guardar error específico
          const errorType = result.error || 'unknown'
          if (!this.stats.specificErrors[errorType]) {
            this.stats.specificErrors[errorType] = 0
          }
          this.stats.specificErrors[errorType]++
        }
      }

      // Guardar detalles para análisis posterior
      this.allErrorDetails.push(...failedDocs)

      await this.writeLog('SUCCESS', `Batch detallado: ${indexed} éxitos, ${failed} fallos`, {
        successfulDocs: successfulDocs.length,
        failedDocs: failedDocs.length,
        transformErrors: transformErrors.length
      })

      return {
        indexed,
        failed: failed + transformErrors.length,
        errors: [...transformErrors, ...indexErrors],
        details: {
          successful: successfulDocs,
          failed: failedDocs,
          transformErrors
        }
      }
    } catch (error) {
      console.log(`💥 ERROR EN BATCH COMPLETO: ${error.message}`)

      // Si hay error en el import, analizar qué pasó
      if (error.importResults && Array.isArray(error.importResults)) {
        console.log(`\n🔍 ANALIZANDO ${error.importResults.length} RESULTADOS DE IMPORT:`)

        error.importResults.forEach((result, i) => {
          if (result.success) {
            console.log(`  ✅ ${i + 1}. Documento exitoso`)
          } else {
            console.log(`  ❌ ${i + 1}. Error: ${result.error}`)
            console.log(`       Código: ${result.code}`)
            if (result.document) {
              console.log(`       Documento: ${result.document.substring(0, 100)}...`)
            }
          }
        })

        // Guardar todos los detalles
        this.allErrorDetails.push(...error.importResults.filter(r => !r.success))
      }

      await this.writeLog('ERROR', 'Error en batch completo', {
        error: error.message,
        importResults: error.importResults || null
      })

      return {
        indexed: 0,
        failed: products.length,
        errors: [{ error: error.message, fullError: error }],
        details: {
          batchError: error,
          importResults: error.importResults || null
        }
      }
    }
  }

  async debugProducts () {
    let currentPage = this.startPage
    let pagesProcessed = 0

    console.log('🔍 Iniciando debug detallado de productos...')
    await this.writeLog('INFO', 'Debug detallado iniciado')

    while (pagesProcessed < this.maxPages) {
      try {
        console.log(`\n📄 PROCESANDO PÁGINA ${currentPage}:`)

        const apiResponse = await this.fetchProductsFromAPI(currentPage)

        if (!apiResponse.success || !apiResponse.products.length) {
          console.log('✅ No hay más productos para procesar')
          break
        }

        console.log(`📦 Obtenidos ${apiResponse.products.length} productos de la API`)

        // Procesar en batches pequeños
        const batches = this.chunkArray(apiResponse.products, this.batchSize)

        for (let i = 0; i < batches.length; i++) {
          const batch = batches[i]
          console.log(`\n🔄 PROCESANDO BATCH ${i + 1}/${batches.length}:`)

          const batchStart = performance.now()
          const result = await this.indexProductBatchDetailed(batch)
          const batchTime = (performance.now() - batchStart) / 1000

          // Actualizar estadísticas
          this.stats.processedProducts += batch.length
          this.stats.indexedProducts += result.indexed
          this.stats.failedProducts += result.failed
          this.stats.batches++

          console.log(`\n📊 RESULTADO BATCH ${i + 1}:`)
          console.log(`  ⏱️  Tiempo: ${batchTime.toFixed(2)}s`)
          console.log(`  ✅ Indexados: ${result.indexed}`)
          console.log(`  ❌ Fallidos: ${result.failed}`)
          console.log(`  📈 Tasa éxito: ${((result.indexed / batch.length) * 100).toFixed(1)}%`)

          // Pausa entre batches
          if (i < batches.length - 1) {
            await this.sleep(1000)
          }
        }

        currentPage++
        pagesProcessed++

        // Pausa entre páginas
        await this.sleep(500)
      } catch (error) {
        console.log(`💥 ERROR EN PÁGINA ${currentPage}: ${error.message}`)
        await this.writeLog('ERROR', `Error en página ${currentPage}`, { error: error.message })

        currentPage++
        pagesProcessed++
        await this.sleep(2000)
      }
    }
  }

  async generateDetailedReport () {
    this.stats.endTime = performance.now()
    const duration = (this.stats.endTime - this.stats.startTime) / 1000

    // Analizar tipos de errores
    const errorAnalysis = {}
    this.allErrorDetails.forEach(error => {
      const errorType = error.error || 'unknown'
      if (!errorAnalysis[errorType]) {
        errorAnalysis[errorType] = {
          count: 0,
          examples: []
        }
      }
      errorAnalysis[errorType].count++
      if (errorAnalysis[errorType].examples.length < 3) {
        errorAnalysis[errorType].examples.push({
          productId: error.productId,
          title: error.title,
          errorCode: error.errorCode
        })
      }
    })

    const report = {
      summary: {
        duration: `${duration.toFixed(2)} segundos`,
        processedProducts: this.stats.processedProducts,
        indexedProducts: this.stats.indexedProducts,
        failedProducts: this.stats.failedProducts,
        batches: this.stats.batches,
        successRate: `${((this.stats.indexedProducts / (this.stats.processedProducts || 1)) * 100).toFixed(2)}%`
      },
      errorAnalysis,
      specificErrors: this.stats.specificErrors,
      allErrorDetails: this.allErrorDetails,
      documentSamples: this.documentSamples,
      timestamp: new Date().toISOString()
    }

    // Guardar reporte detallado
    await fs.writeFile(this.errorDetailsFile, JSON.stringify(report, null, 2))

    // Guardar samples de documentos
    await fs.writeFile(this.documentsFile, JSON.stringify(this.documentSamples, null, 2))

    console.log('\n📊 RESUMEN DE DEBUG DETALLADO:')
    console.log('='.repeat(60))
    console.log(`⏱️  Duración: ${report.summary.duration}`)
    console.log(`📦 Productos procesados: ${report.summary.processedProducts}`)
    console.log(`✅ Productos indexados: ${report.summary.indexedProducts}`)
    console.log(`❌ Productos fallidos: ${report.summary.failedProducts}`)
    console.log(`📈 Tasa de éxito: ${report.summary.successRate}`)

    console.log('\n🐛 ANÁLISIS DE ERRORES:')
    Object.entries(errorAnalysis).forEach(([errorType, data]) => {
      console.log(`  ❌ ${errorType}: ${data.count} ocurrencias`)
      data.examples.forEach(example => {
        console.log(`     → ${example.productId}: ${example.title?.substring(0, 50)}...`)
      })
    })

    console.log('\n📁 ARCHIVOS GENERADOS:')
    console.log(`📋 Log detallado: ${this.logFile}`)
    console.log(`🐛 Detalles de errores: ${this.errorDetailsFile}`)
    console.log(`📄 Samples de documentos: ${this.documentsFile}`)
    console.log('='.repeat(60))

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
  const indexerDebug = new TypesenseDetailedDebugger()

  try {
    await indexerDebug.init()
    await indexerDebug.debugProducts()
    await indexerDebug.generateDetailedReport()
  } catch (error) {
    console.error('💥 Error fatal:', error.message)
    await indexerDebug.writeLog('ERROR', 'Error fatal', { error: error.message })
    process.exit(1)
  }
}

// Ejecutar
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error)
}

export { TypesenseDetailedDebugger }
