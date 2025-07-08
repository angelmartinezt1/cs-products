// scripts/run-indexer.js
import { program } from 'commander'
import mysql from 'mysql2/promise'
import WorkingBulkService from '../src/services/WorkingBulkService.js'
import { ProductIndexer } from './product-indexer.js'

// Configuración CLI
program
  .name('run-indexer')
  .description('Script para indexar productos desde la API de Sears')
  .version('1.0.0')

program
  .option('-p, --pages <number>', 'Número máximo de páginas a procesar', '1')
  .option('-s, --start-page <number>', 'Página inicial', '1')
  .option('-b, --batch-size <number>', 'Tamaño de lote para BD', '100')
  .option('--api-page-size <number>', 'Tamaño de página de la API', '100')
  .option('--update-facets', 'Actualizar facetas después de indexar', false)
  .option('--cleanup-old', 'Limpiar productos antiguos', false)
  .option('--optimize-tables', 'Optimizar tablas después de indexar', false)
  .option('--stats-only', 'Solo mostrar estadísticas', false)
  .option('--dry-run', 'Simulación sin escribir a BD', false)

program.parse()

const options = program.opts()

class IndexerRunner {
  constructor (options) {
    this.options = {
      maxPages: parseInt(options.pages) || 1,
      startPage: parseInt(options.startPage) || 1,
      batchSize: parseInt(options.batchSize) || 100,
      apiPageSize: parseInt(options.apiPageSize) || 100,
      updateFacets: options.updateFacets || false,
      cleanupOld: options.cleanupOld || false,
      optimizeTables: options.optimizeTables || false,
      statsOnly: options.statsOnly || false,
      dryRun: options.dryRun || false
    }

    this.connection = null
    this.bulkService = null
  }

  async init () {
    console.log('🚀 Iniciando Product Indexer Runner...')
    console.log('📋 Configuración:', this.options)

    try {
      this.connection = await mysql.createConnection({
        host: process.env.DB_HOST || 'localhost',
        user: process.env.DB_USER || 'root',
        password: process.env.DB_PASSWORD || '',
        database: process.env.DB_NAME || 'search_products',
        charset: 'utf8mb4',
        multipleStatements: true,
        acquireTimeout: 60000,
        timeout: 60000
      })

      this.bulkService = new WorkingBulkService(this.connection)
      this.bulkService.batchSize = this.options.batchSize

      console.log('✅ Conexión establecida')
    } catch (error) {
      console.error('❌ Error de conexión:', error.message)
      throw error
    }
  }

  async showStats () {
    console.log('📊 Obteniendo estadísticas actuales...')
    try {
      const stats = await this.bulkService.getDatabaseStats()

      console.log('\n📈 ESTADÍSTICAS ACTUALES:')
      console.log('='.repeat(50))
      console.log(`📦 Total productos: ${stats.products.total_products.toLocaleString()}`)
      console.log(`✅ Productos activos: ${stats.products.active_products.toLocaleString()}`)
      console.log(`🏷️  Marcas únicas: ${stats.products.unique_brands.toLocaleString()}`)
      console.log(`🏪 Tiendas únicas: ${stats.products.unique_stores.toLocaleString()}`)
      console.log(`📂 Categorías únicas: ${stats.products.unique_categories.toLocaleString()}`)
      console.log(`💰 Precio promedio: $${parseFloat(stats.products.avg_price || 0).toFixed(2)}`)
      console.log(`📦 Stock total: ${parseInt(stats.products.total_stock || 0).toLocaleString()}`)
      console.log(`🖼️  Imágenes: ${stats.images.total_images.toLocaleString()}`)
      console.log(`🎨 Variaciones: ${stats.variations.total_variations.toLocaleString()}`)
      console.log(`📋 Atributos: ${stats.attributes.total_attributes.toLocaleString()}`)
      console.log(`🔍 Facetas: ${stats.facets.total_facets.toLocaleString()}`)

      if (stats.facets.last_facet_update) {
        const lastUpdate = new Date(stats.facets.last_facet_update)
        console.log(`🕐 Última actualización facetas: ${lastUpdate.toLocaleString()}`)
      }
      console.log('='.repeat(50))
    } catch (error) {
      console.error('❌ Error obteniendo estadísticas:', error.message)
    }
  }

  async runIndexer () {
    if (this.options.dryRun) {
      console.log('🔍 MODO SIMULACIÓN ACTIVADO - No se escribirá a la BD')
    }

    // Crear indexer personalizado
    const indexer = new ProductIndexer()
    indexer.connection = this.connection

    try {
      console.log('📡 Iniciando indexación de productos...')

      let currentPage = this.options.startPage
      let hasMorePages = true
      let totalProcessed = 0

      while (hasMorePages) {
        // Verificar límite de páginas
        if (this.options.maxPages > 0 && currentPage > (this.options.startPage + this.options.maxPages - 1)) {
          console.log(`🛑 Límite de páginas alcanzado (${this.options.maxPages} páginas)`)
          break
        }

        console.log(`📄 Procesando página ${currentPage}...`)

        try {
          const apiResponse = await indexer.fetchProductsFromAPI(currentPage)

          if (!apiResponse.success || !apiResponse.products.length) {
            console.log('✅ No hay más productos para procesar')
            break
          }

          console.log(`📦 Obtenidos ${apiResponse.products.length} productos`)

          if (!this.options.dryRun) {
            // Transformar productos
            const transformedProducts = apiResponse.products.map(p => {
              const transformed = indexer.transformProduct(p)
              return transformed
            })

            // Insertar usando el servicio bulk
            const insertResult = await this.bulkService.bulkUpsertProducts(transformedProducts)

            console.log(`✅ Página ${currentPage}: ${insertResult.inserted} insertados, ${insertResult.updated} actualizados, ${insertResult.errors} errores`)

            totalProcessed += apiResponse.products.length
          } else {
            console.log(`🔍 [SIMULACIÓN] Se procesarían ${apiResponse.products.length} productos`)
            totalProcessed += apiResponse.products.length
          }

          // Verificar paginación
          if (apiResponse.pagination) {
            hasMorePages = currentPage < (apiResponse.pagination.pageCount || 0)
          } else {
            hasMorePages = false
          }

          currentPage++

          // Pausa entre páginas
          await this.sleep(100)
        } catch (error) {
          console.error(`❌ Error en página ${currentPage}:`, error.message)
          currentPage++

          // Si hay muchos errores consecutivos, abortar
          if (currentPage - this.options.startPage > 5) {
            console.error('💥 Demasiados errores consecutivos, abortando...')
            break
          }

          // Pausa más larga en caso de error
          await this.sleep(2000)
        }
      }

      console.log(`📊 Total procesado: ${totalProcessed} productos`)
    } catch (error) {
      console.error('💥 Error durante indexación:', error.message)
      throw error
    }
  }

  async runMaintenance () {
    if (this.options.cleanupOld) {
      console.log('🧹 Ejecutando limpieza de productos antiguos...')
      try {
        const cleanupResult = await this.bulkService.cleanupOldProducts(30)
        console.log(`✅ Limpieza completada: ${cleanupResult.deleted} productos eliminados`)
      } catch (error) {
        console.error('❌ Error en limpieza:', error.message)
      }
    }

    if (this.options.updateFacets) {
      console.log('🔄 Actualizando facetas...')
      try {
        await this.bulkService.updateFacets()
        console.log('✅ Facetas actualizadas')
      } catch (error) {
        console.error('❌ Error actualizando facetas:', error.message)
      }
    }

    if (this.options.optimizeTables) {
      console.log('⚡ Optimizando tablas...')
      try {
        const optimizeResult = await this.bulkService.optimizeTables()
        console.log('✅ Optimización completada')

        optimizeResult.results.forEach(result => {
          if (result.success) {
            console.log(`  ✅ ${result.table}: ${result.duration.toFixed(2)}s`)
          } else {
            console.log(`  ❌ ${result.table}: ${result.error}`)
          }
        })
      } catch (error) {
        console.error('❌ Error optimizando tablas:', error.message)
      }
    }
  }

  sleep (ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
  }

  async run () {
    try {
      await this.init()

      // Mostrar estadísticas iniciales
      await this.showStats()

      if (this.options.statsOnly) {
        console.log('📊 Solo estadísticas solicitadas, terminando...')
        return
      }

      // Ejecutar indexación
      await this.runIndexer()

      // Ejecutar mantenimiento
      await this.runMaintenance()

      // Mostrar estadísticas finales
      console.log('\n📊 ESTADÍSTICAS FINALES:')
      await this.showStats()
    } catch (error) {
      console.error('💥 Error fatal:', error.message)
      process.exit(1)
    } finally {
      if (this.connection) {
        await this.connection.end()
        console.log('🔌 Conexión cerrada')
      }
    }
  }
}

// Ejecutar
const runner = new IndexerRunner(options)
runner.run().catch(console.error)

export { IndexerRunner }
