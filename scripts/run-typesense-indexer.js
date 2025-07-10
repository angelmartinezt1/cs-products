// scripts/run-typesense-indexer.js
import axios from 'axios'
import { program } from 'commander'
import { TypesenseIndexer } from './typesense-indexer.js'

// Configuración CLI
program
  .name('run-typesense-indexer')
  .description('Script para indexar productos en Typesense')
  .version('1.0.0')

program
  .option('-p, --pages <number>', 'Número máximo de páginas a procesar', '0')
  .option('-s, --start-page <number>', 'Página inicial', '1')
  .option('-b, --batch-size <number>', 'Tamaño de lote para Typesense', '50')
  .option('--api-page-size <number>', 'Tamaño de página de la API', '100')
  .option('--collection-name <string>', 'Nombre de la colección en Typesense', 'products')
  .option('--recreate-collection', 'Recrear la colección (elimina datos existentes)', false)
  .option('--stats-only', 'Solo mostrar estadísticas de la colección', false)
  .option('--dry-run', 'Simulación sin indexar', false)
  .option('--test-connection', 'Solo probar conexión con Typesense', false)

program.parse()

const options = program.opts()

class TypesenseIndexerRunner {
  constructor (options) {
    this.options = {
      maxPages: parseInt(options.pages) || 0,
      startPage: parseInt(options.startPage) || 1,
      batchSize: parseInt(options.batchSize) || 50,
      apiPageSize: parseInt(options.apiPageSize) || 100,
      collectionName: options.collectionName || 'products',
      recreateCollection: options.recreateCollection || false,
      statsOnly: options.statsOnly || false,
      dryRun: options.dryRun || false,
      testConnection: options.testConnection || false
    }

    this.typesenseConfig = {
      host: process.env.TYPESENSE_HOST || 'localhost',
      port: process.env.TYPESENSE_PORT || '8108',
      protocol: process.env.TYPESENSE_PROTOCOL || 'http',
      apiKey: process.env.TYPESENSE_API_KEY || 'cs-products-search-supersecret-key-2024'
    }

    this.baseUrl = `${this.typesenseConfig.protocol}://${this.typesenseConfig.host}:${this.typesenseConfig.port}`
    this.headers = {
      'X-TYPESENSE-API-KEY': this.typesenseConfig.apiKey,
      'Content-Type': 'application/json'
    }
  }

  async run () {
    console.log('🚀 Iniciando Typesense Indexer Runner...')
    console.log('📋 Configuración:', this.options)

    try {
      // Test de conexión
      if (this.options.testConnection) {
        await this.testConnection()
        return
      }

      // Solo estadísticas
      if (this.options.statsOnly) {
        await this.showCollectionStats()
        return
      }

      // Recrear colección si se solicita
      if (this.options.recreateCollection) {
        await this.recreateCollection()
      }

      // Modo dry-run
      if (this.options.dryRun) {
        await this.runDryMode()
        return
      }

      // Ejecutar indexación real
      await this.runIndexer()
    } catch (error) {
      console.error('💥 Error fatal:', error.message)
      process.exit(1)
    }
  }

  async testConnection () {
    console.log('🔌 Probando conexión con Typesense...')

    try {
      const response = await axios.get(`${this.baseUrl}/health`, {
        headers: { 'X-TYPESENSE-API-KEY': this.typesenseConfig.apiKey },
        timeout: 5000
      })

      if (response.data.ok === true) {
        console.log('✅ Conexión exitosa con Typesense')
        console.log(`📍 Servidor: ${this.baseUrl}`)
        console.log(`🔑 API Key: ${this.typesenseConfig.apiKey.substring(0, 10)}...`)

        // Listar colecciones
        try {
          const collectionsResponse = await axios.get(`${this.baseUrl}/collections`, {
            headers: this.headers
          })

          console.log(`📁 Colecciones disponibles: ${collectionsResponse.data.length}`)
          collectionsResponse.data.forEach(collection => {
            console.log(`  - ${collection.name} (${collection.num_documents} documentos)`)
          })
        } catch (error) {
          console.log('⚠️ No se pudieron listar las colecciones')
        }
      } else {
        throw new Error('Respuesta inválida del servidor')
      }
    } catch (error) {
      console.error('❌ Error de conexión:', error.message)
      throw error
    }
  }

  async showCollectionStats () {
    console.log('📊 Obteniendo estadísticas de la colección...')

    try {
      const response = await axios.get(`${this.baseUrl}/collections/${this.options.collectionName}`, {
        headers: this.headers
      })

      const collection = response.data

      console.log('\n📈 ESTADÍSTICAS DE LA COLECCIÓN:')
      console.log('='.repeat(50))
      console.log(`📂 Nombre: ${collection.name}`)
      console.log(`📦 Documentos: ${collection.num_documents.toLocaleString()}`)
      console.log(`📋 Campos: ${collection.fields?.length || 0}`)
      console.log(`🔍 Campo de ordenamiento: ${collection.default_sorting_field || 'ninguno'}`)
      console.log(`🕐 Creada: ${new Date(collection.created_at * 1000).toLocaleString()}`)

      if (collection.fields) {
        const facetFields = collection.fields.filter(f => f.facet).map(f => f.name)
        console.log(`🏷️  Campos con facetas: ${facetFields.length > 0 ? facetFields.join(', ') : 'ninguno'}`)
      }

      console.log('='.repeat(50))
    } catch (error) {
      if (error.response?.status === 404) {
        console.log(`❌ La colección '${this.options.collectionName}' no existe`)
      } else {
        console.error('❌ Error obteniendo estadísticas:', error.message)
      }
    }
  }

  async recreateCollection () {
    console.log(`🔄 Recreando colección '${this.options.collectionName}'...`)

    try {
      // Eliminar colección existente
      try {
        await axios.delete(`${this.baseUrl}/collections/${this.options.collectionName}`, {
          headers: this.headers
        })
        console.log('✅ Colección anterior eliminada')
      } catch (error) {
        if (error.response?.status === 404) {
          console.log('ℹ️ La colección no existía previamente')
        } else {
          throw error
        }
      }

      // Esperar un momento para que Typesense procese la eliminación
      await this.sleep(1000)

      console.log('✅ Colección recreada (se creará automáticamente en la indexación)')
    } catch (error) {
      console.error('❌ Error recreando colección:', error.message)
      throw error
    }
  }

  async runDryMode () {
    console.log('🔍 MODO SIMULACIÓN ACTIVADO - No se indexará en Typesense')

    const API_CONFIG = {
      baseUrl: 'https://csapi.claroshop.com/products/v1/products/',
      pageSize: this.options.apiPageSize,
      timeout: 30000
    }

    let currentPage = this.options.startPage
    let totalProcessed = 0
    let pagesProcessed = 0

    try {
      while (true) {
        // Verificar límite de páginas
        if (this.options.maxPages > 0 && pagesProcessed >= this.options.maxPages) {
          console.log(`🛑 Límite de páginas alcanzado (${this.options.maxPages} páginas)`)
          break
        }

        console.log(`📄 [SIMULACIÓN] Obteniendo página ${currentPage}...`)

        const url = `${API_CONFIG.baseUrl}?page_size=${API_CONFIG.pageSize}&page=${currentPage}`
        const response = await axios.get(url, {
          timeout: API_CONFIG.timeout,
          headers: {
            'User-Agent': 'TypesenseIndexer-DryRun/1.0',
            Accept: 'application/json'
          }
        })

        if (!response.data?.metadata || response.data.metadata.is_error !== false) {
          console.log('✅ No hay más productos para procesar')
          break
        }

        const products = response.data.data || []
        console.log(`📦 [SIMULACIÓN] Se procesarían ${products.length} productos`)

        // Simular procesamiento por batches
        const batches = Math.ceil(products.length / this.options.batchSize)
        console.log(`📊 [SIMULACIÓN] Se dividirían en ${batches} batches de ${this.options.batchSize}`)

        totalProcessed += products.length
        pagesProcessed++
        currentPage++

        // Verificar paginación
        if (response.data.pagination) {
          const hasMorePages = currentPage <= (response.data.pagination.pageCount || 0)
          if (!hasMorePages) {
            console.log('✅ No hay más páginas disponibles')
            break
          }
        }

        await this.sleep(100)
      }

      console.log('\n📊 RESUMEN DE SIMULACIÓN:')
      console.log('='.repeat(40))
      console.log(`📄 Páginas procesadas: ${pagesProcessed}`)
      console.log(`📦 Total productos: ${totalProcessed}`)
      console.log(`📊 Batches estimados: ${Math.ceil(totalProcessed / this.options.batchSize)}`)
      console.log('='.repeat(40))
    } catch (error) {
      console.error('❌ Error en simulación:', error.message)
    }
  }

  async runIndexer () {
    // Crear indexer con configuración personalizada
    const indexer = new TypesenseIndexer()

    // Sobrescribir configuraciones
    indexer.batchSize = this.options.batchSize

    // Actualizar configuración de API si es necesario
    if (this.options.apiPageSize !== 100) {
      const API_CONFIG = {
        baseUrl: 'https://csapi.claroshop.com/products/v1/products/',
        pageSize: this.options.apiPageSize,
        maxRetries: 3,
        retryDelay: 1000,
        timeout: 30000
      }
      // Esto requeriría modificar el indexer para aceptar configuración externa
    }

    try {
      await indexer.init()

      // Indexar con límites si se especificaron
      if (this.options.maxPages > 0) {
        await this.runLimitedIndexing(indexer)
      } else {
        await indexer.indexAllProducts()
      }

      await indexer.generateReport()

      // Mostrar estadísticas finales
      await this.showCollectionStats()
    } catch (error) {
      console.error('❌ Error durante indexación:', error.message)
      throw error
    }
  }

  async runLimitedIndexing (indexer) {
    let currentPage = this.options.startPage
    let pagesProcessed = 0

    console.log(`📊 Indexación limitada: ${this.options.maxPages} páginas desde la página ${this.options.startPage}`)

    while (pagesProcessed < this.options.maxPages) {
      try {
        const apiResponse = await indexer.fetchProductsFromAPI(currentPage)

        if (!apiResponse.success || !apiResponse.products.length) {
          console.log('✅ No hay más productos para procesar')
          break
        }

        console.log(`📦 Procesando ${apiResponse.products.length} productos de la página ${currentPage}`)

        // Procesar en batches
        const batches = indexer.chunkArray(apiResponse.products, indexer.batchSize)

        for (let i = 0; i < batches.length; i++) {
          const batch = batches[i]
          const batchStart = performance.now()

          const result = await indexer.indexProductBatch(batch)

          // Actualizar estadísticas del indexer
          indexer.stats.processedProducts += batch.length
          indexer.stats.indexedProducts += result.indexed
          indexer.stats.failedProducts += result.failed
          indexer.stats.batches++
          indexer.stats.errors.push(...result.errors)

          const batchTime = (performance.now() - batchStart) / 1000
          const speed = (batch.length / batchTime).toFixed(2)

          await indexer.writeLog('SUCCESS',
            `Batch ${i + 1}/${batches.length} completado: ${result.indexed} indexados, ${result.failed} fallidos en ${batchTime.toFixed(2)}s (${speed} p/s)`
          )

          if (i < batches.length - 1) {
            await indexer.sleep(200)
          }
        }

        pagesProcessed++
        currentPage++
        await indexer.sleep(100)
      } catch (error) {
        await indexer.writeLog('ERROR', `Error procesando página ${currentPage}`, { error: error.message })
        indexer.stats.errors.push({ page: currentPage, error: error.message })

        pagesProcessed++
        currentPage++
        await indexer.sleep(2000)
      }
    }

    console.log(`✅ Indexación limitada completada: ${pagesProcessed} páginas procesadas`)
  }

  sleep (ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
  }
}

// Ejecutar
const runner = new TypesenseIndexerRunner(options)
runner.run().catch(console.error)

export { TypesenseIndexerRunner }
