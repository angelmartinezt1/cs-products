// scripts/typesense-utils.js
import axios from 'axios'
import fs from 'fs/promises'
import { performance } from 'perf_hooks'

class TypesenseUtils {
  constructor () {
    this.config = {
      host: process.env.TYPESENSE_HOST || 'localhost',
      port: process.env.TYPESENSE_PORT || '8108',
      protocol: process.env.TYPESENSE_PROTOCOL || 'http',
      apiKey: process.env.TYPESENSE_API_KEY || 'cs-products-search-supersecret-key-2024',
      collectionName: process.env.TYPESENSE_COLLECTION || 'products'
    }

    this.baseUrl = `${this.config.protocol}://${this.config.host}:${this.config.port}`
    this.headers = {
      'X-TYPESENSE-API-KEY': this.config.apiKey,
      'Content-Type': 'application/json'
    }
  }

  async getCollectionInfo () {
    try {
      const response = await axios.get(`${this.baseUrl}/collections/${this.config.collectionName}`, {
        headers: this.headers
      })
      return response.data
    } catch (error) {
      if (error.response?.status === 404) {
        return null
      }
      throw error
    }
  }

  async searchProducts (query, options = {}) {
    const searchParams = {
      q: query || '*',
      query_by: 'title,description,brand',
      sort_by: options.sortBy || 'relevance_score:desc',
      per_page: options.limit || 20,
      page: options.page || 1,
      facet_by: options.facetBy || 'brand,hirerarchical_category.lvl0,hirerarchical_category.lvl1,fulfillment,has_free_shipping',
      filter_by: options.filterBy || '',
      ...options.extraParams
    }

    try {
      const response = await axios.get(`${this.baseUrl}/collections/${this.config.collectionName}/documents/search`, {
        headers: this.headers,
        params: searchParams
      })

      return {
        hits: response.data.hits || [],
        facets: response.data.facet_counts || [],
        found: response.data.found || 0,
        searchTimeMs: response.data.search_time_ms || 0,
        page: response.data.page || 1
      }
    } catch (error) {
      console.error('Error en búsqueda:', error.message)
      throw error
    }
  }

  async getDocument (id) {
    try {
      const response = await axios.get(`${this.baseUrl}/collections/${this.config.collectionName}/documents/${id}`, {
        headers: this.headers
      })
      return response.data
    } catch (error) {
      if (error.response?.status === 404) {
        return null
      }
      throw error
    }
  }

  async deleteDocument (id) {
    try {
      await axios.delete(`${this.baseUrl}/collections/${this.config.collectionName}/documents/${id}`, {
        headers: this.headers
      })
      return true
    } catch (error) {
      console.error(`Error eliminando documento ${id}:`, error.message)
      return false
    }
  }

  async bulkDelete (ids) {
    const results = { deleted: 0, failed: 0, errors: [] }

    for (const id of ids) {
      try {
        const success = await this.deleteDocument(id)
        if (success) {
          results.deleted++
        } else {
          results.failed++
        }
      } catch (error) {
        results.failed++
        results.errors.push({ id, error: error.message })
      }
    }

    return results
  }

  async exportCollection (filePath = null) {
    try {
      console.log('📥 Exportando colección...')
      const startTime = performance.now()

      const response = await axios.get(`${this.baseUrl}/collections/${this.config.collectionName}/documents/export`, {
        headers: this.headers
      })

      const documents = response.data.split('\n').filter(line => line.trim()).map(line => JSON.parse(line))

      const exportData = {
        collection: this.config.collectionName,
        exported_at: new Date().toISOString(),
        document_count: documents.length,
        documents
      }

      const fileName = filePath || `exports/typesense-export-${Date.now()}.json`

      // Crear directorio si no existe
      await fs.mkdir('exports', { recursive: true })

      await fs.writeFile(fileName, JSON.stringify(exportData, null, 2))

      const duration = (performance.now() - startTime) / 1000
      console.log(`✅ Exportación completada: ${documents.length} documentos en ${duration.toFixed(2)}s`)
      console.log(`📄 Archivo: ${fileName}`)

      return {
        fileName,
        documentCount: documents.length,
        duration
      }
    } catch (error) {
      console.error('❌ Error en exportación:', error.message)
      throw error
    }
  }

  async importFromFile (filePath) {
    try {
      console.log(`📤 Importando desde ${filePath}...`)
      const startTime = performance.now()

      const fileContent = await fs.readFile(filePath, 'utf8')
      const data = JSON.parse(fileContent)

      if (!data.documents || !Array.isArray(data.documents)) {
        throw new Error('Archivo de importación inválido')
      }

      const documents = data.documents
      const batchSize = 50
      let imported = 0
      let failed = 0

      console.log(`📦 Importando ${documents.length} documentos en batches de ${batchSize}...`)

      for (let i = 0; i < documents.length; i += batchSize) {
        const batch = documents.slice(i, i + batchSize)
        const importData = batch.map(doc => JSON.stringify(doc)).join('\n')

        try {
          const response = await axios.post(
            `${this.baseUrl}/collections/${this.config.collectionName}/documents/import?action=upsert`,
            importData,
            {
              headers: {
                ...this.headers,
                'Content-Type': 'application/jsonl'
              }
            }
          )

          const results = response.data.split('\n').filter(line => line.trim())

          for (const result of results) {
            const parsed = JSON.parse(result)
            if (parsed.success) {
              imported++
            } else {
              failed++
            }
          }

          console.log(`✅ Batch ${Math.floor(i / batchSize) + 1}: ${batch.length} documentos procesados`)
        } catch (error) {
          console.error(`❌ Error en batch ${Math.floor(i / batchSize) + 1}:`, error.message)
          failed += batch.length
        }
      }

      const duration = (performance.now() - startTime) / 1000
      console.log(`✅ Importación completada: ${imported} importados, ${failed} fallidos en ${duration.toFixed(2)}s`)

      return { imported, failed, duration }
    } catch (error) {
      console.error('❌ Error en importación:', error.message)
      throw error
    }
  }

  async getCollectionStats () {
    try {
      const info = await this.getCollectionInfo()
      if (!info) {
        return { exists: false }
      }

      // Hacer algunas consultas de estadísticas
      const [totalDocs, activeDocs, brandsCount] = await Promise.all([
        this.searchProducts('*', { limit: 1 }),
        this.searchProducts('*', { limit: 1, filterBy: 'is_active:true' }),
        this.searchProducts('*', { limit: 1, facetBy: 'brand' })
      ])

      const brandFacets = brandsCount.facets.find(f => f.field_name === 'brand')
      const uniqueBrands = brandFacets ? brandFacets.counts.length : 0

      return {
        exists: true,
        name: info.name,
        totalDocuments: totalDocs.found,
        activeDocuments: activeDocs.found,
        uniqueBrands,
        fields: info.fields?.length || 0,
        createdAt: new Date(info.created_at * 1000).toISOString(),
        defaultSortingField: info.default_sorting_field
      }
    } catch (error) {
      console.error('❌ Error obteniendo estadísticas:', error.message)
      throw error
    }
  }

  async cleanupInactiveProducts () {
    try {
      console.log('🧹 Limpiando productos inactivos...')
      const startTime = performance.now()

      // Buscar productos inactivos
      const inactiveProducts = await this.searchProducts('*', {
        filterBy: 'is_active:false',
        limit: 1000, // Procesar en lotes
        page: 1
      })

      if (inactiveProducts.found === 0) {
        console.log('✅ No hay productos inactivos para limpiar')
        return { deleted: 0, duration: 0 }
      }

      console.log(`🗑️ Encontrados ${inactiveProducts.found} productos inactivos`)

      const ids = inactiveProducts.hits.map(hit => hit.document.objectID)
      const deleteResult = await this.bulkDelete(ids)

      const duration = (performance.now() - startTime) / 1000
      console.log(`✅ Limpieza completada: ${deleteResult.deleted} eliminados, ${deleteResult.failed} fallidos en ${duration.toFixed(2)}s`)

      return {
        deleted: deleteResult.deleted,
        failed: deleteResult.failed,
        duration
      }
    } catch (error) {
      console.error('❌ Error en limpieza:', error.message)
      throw error
    }
  }

  async validateCollection () {
    try {
      console.log('🔍 Validando colección...')

      const info = await this.getCollectionInfo()
      if (!info) {
        console.log('❌ La colección no existe')
        return false
      }

      console.log('✅ Colección existe')
      console.log(`📊 Documentos: ${info.num_documents}`)
      console.log(`📋 Campos: ${info.fields?.length || 0}`)

      // Verificar campos requeridos
      const requiredFields = ['objectID', 'product_id', 'title', 'price', 'is_active']
      const existingFields = info.fields?.map(f => f.name) || []

      const missingFields = requiredFields.filter(field => !existingFields.includes(field))

      if (missingFields.length > 0) {
        console.log(`⚠️ Campos faltantes: ${missingFields.join(', ')}`)
        return false
      }

      console.log('✅ Todos los campos requeridos están presentes')

      // Hacer una búsqueda de prueba
      const testSearch = await this.searchProducts('test', { limit: 1 })
      console.log(`✅ Búsqueda de prueba exitosa (${testSearch.searchTimeMs}ms)`)

      return true
    } catch (error) {
      console.error('❌ Error validando colección:', error.message)
      return false
    }
  }

  async benchmarkSearch (queries = ['*', 'producto', 'televisor', 'samsung'], iterations = 10) {
    console.log(`🏃 Ejecutando benchmark de búsqueda (${iterations} iteraciones por query)...`)

    const results = []

    for (const query of queries) {
      const times = []

      for (let i = 0; i < iterations; i++) {
        const start = performance.now()
        await this.searchProducts(query, { limit: 20 })
        const duration = performance.now() - start
        times.push(duration)
      }

      const avgTime = times.reduce((a, b) => a + b, 0) / times.length
      const minTime = Math.min(...times)
      const maxTime = Math.max(...times)

      results.push({
        query,
        avgTimeMs: Math.round(avgTime * 100) / 100,
        minTimeMs: Math.round(minTime * 100) / 100,
        maxTimeMs: Math.round(maxTime * 100) / 100,
        iterations
      })

      console.log(`📊 "${query}": avg ${Math.round(avgTime)}ms, min ${Math.round(minTime)}ms, max ${Math.round(maxTime)}ms`)
    }

    return results
  }
}

export { TypesenseUtils }

// CLI para usar las utilidades
if (import.meta.url === `file://${process.argv[1]}`) {
  const utils = new TypesenseUtils()
  const command = process.argv[2]

  switch (command) {
    case 'stats':
      utils.getCollectionStats().then(stats => {
        console.log('📊 Estadísticas:', JSON.stringify(stats, null, 2))
      }).catch(console.error)
      break

    case 'export':
      utils.exportCollection(process.argv[3]).catch(console.error)
      break

    case 'import':
      if (!process.argv[3]) {
        console.error('❌ Especifica el archivo a importar')
        process.exit(1)
      }
      utils.importFromFile(process.argv[3]).catch(console.error)
      break

    case 'cleanup':
      utils.cleanupInactiveProducts().catch(console.error)
      break

    case 'validate':
      utils.validateCollection().catch(console.error)
      break

    case 'benchmark':
      utils.benchmarkSearch().catch(console.error)
      break

    case 'search':
      if (!process.argv[3]) {
        console.error('❌ Especifica una query de búsqueda')
        process.exit(1)
      }
      utils.searchProducts(process.argv[3], { limit: 10 }).then(results => {
        console.log(`🔍 Encontrados ${results.found} resultados en ${results.searchTimeMs}ms`)
        results.hits.forEach((hit, i) => {
          console.log(`${i + 1}. ${hit.document.title} - $${hit.document.sale_price}`)
        })
      }).catch(console.error)
      break

    default:
      console.log('📋 Comandos disponibles:')
      console.log('  stats     - Mostrar estadísticas de la colección')
      console.log('  export    - Exportar colección a archivo JSON')
      console.log('  import    - Importar desde archivo JSON')
      console.log('  cleanup   - Limpiar productos inactivos')
      console.log('  validate  - Validar esquema de la colección')
      console.log('  benchmark - Ejecutar benchmark de búsqueda')
      console.log('  search    - Buscar productos')
      console.log('')
      console.log('Ejemplos:')
      console.log('  node typesense-utils.js stats')
      console.log('  node typesense-utils.js export backup.json')
      console.log('  node typesense-utils.js search "samsung tv"')
  }
}
