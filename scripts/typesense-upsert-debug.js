// scripts/typesense-upsert-debug.js
import axios from 'axios'
import fs from 'fs/promises'
import Typesense from 'typesense'

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
  pageSize: 10, // Solo 10 productos para debug
  timeout: 30000
}

class UpsertDebugger {
  constructor () {
    this.client = new Typesense.Client(TYPESENSE_CONFIG)
    this.collectionName = TYPESENSE_CONFIG.collectionName
    this.timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, '-')
    this.logFile = `logs/upsert-debug-${this.timestamp}.log`
  }

  async init () {
    console.log('🔍 DIAGNÓSTICO DE UPSERT - Debug de Duplicados')
    console.log('='.repeat(50))

    await this.ensureLogDirectory()
    await this.writeLog('INFO', 'Upsert debugger iniciado')

    try {
      await this.checkConnection()
      await this.analyzeSchema()
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

  async analyzeSchema () {
    try {
      const collection = await this.client.collections(this.collectionName).retrieve()

      console.log('\n📋 ANÁLISIS DEL ESQUEMA:')
      console.log(`📦 Documentos actuales: ${collection.num_documents}`)
      console.log(`📋 Total campos: ${collection.fields?.length || 0}`)
      console.log(`🎯 Campo de ordenamiento: ${collection.default_sorting_field || 'ninguno'}`)

      // Buscar campo de ID primario
      const idFields = collection.fields.filter(f =>
        f.name.includes('id') || f.name.includes('ID') || f.name === 'objectID'
      )

      console.log('\n🔑 CAMPOS DE ID ENCONTRADOS:')
      idFields.forEach(field => {
        console.log(`  - ${field.name} (${field.type}) ${field.optional ? '🔸opcional' : '🔹obligatorio'}`)
      })

      await this.writeLog('INFO', 'Esquema analizado', {
        totalDocs: collection.num_documents,
        totalFields: collection.fields?.length,
        idFields: idFields.map(f => ({ name: f.name, type: f.type, optional: f.optional }))
      })

      return collection
    } catch (error) {
      throw new Error(`Error analizando esquema: ${error.message}`)
    }
  }

  async getExistingDocuments () {
    try {
      console.log('\n📊 DOCUMENTOS EXISTENTES EN TYPESENSE:')

      const searchResults = await this.client.collections(this.collectionName).documents().search({
        q: '*',
        per_page: 250,
        sort_by: 'indexing_date:desc'
      })

      const docs = searchResults.hits.map(hit => hit.document)

      console.log(`📦 Documentos encontrados: ${docs.length}`)

      if (docs.length > 0) {
        console.log('\n🔍 PRIMEROS 10 DOCUMENTOS:')
        docs.slice(0, 10).forEach((doc, i) => {
          console.log(`  ${i + 1}. ID: ${doc.objectID} | Product ID: ${doc.product_id} | Título: ${doc.title?.substring(0, 50)}...`)
        })

        // Analizar duplicados por objectID
        const objectIDs = docs.map(d => d.objectID)
        const uniqueObjectIDs = [...new Set(objectIDs)]
        const duplicatesByObjectID = objectIDs.length - uniqueObjectIDs.length

        // Analizar duplicados por product_id
        const productIDs = docs.map(d => d.product_id)
        const uniqueProductIDs = [...new Set(productIDs)]
        const duplicatesByProductID = productIDs.length - uniqueProductIDs.length

        console.log('\n📊 ANÁLISIS DE DUPLICADOS:')
        console.log(`🔹 Total documentos: ${docs.length}`)
        console.log(`🔸 Únicos por objectID: ${uniqueObjectIDs.length} (${duplicatesByObjectID} duplicados)`)
        console.log(`🔸 Únicos por product_id: ${uniqueProductIDs.length} (${duplicatesByProductID} duplicados)`)

        if (duplicatesByObjectID > 0) {
          console.log('\n⚠️ DUPLICADOS POR objectID DETECTADOS:')
          const counts = {}
          objectIDs.forEach(id => counts[id] = (counts[id] || 0) + 1)
          Object.entries(counts)
            .filter(([id, count]) => count > 1)
            .slice(0, 5)
            .forEach(([id, count]) => {
              console.log(`  - objectID '${id}' aparece ${count} veces`)
            })
        }
      }

      return docs
    } catch (error) {
      console.log(`❌ Error obteniendo documentos existentes: ${error.message}`)
      return []
    }
  }

  async fetchProductsFromAPI (page = 1) {
    const url = `${API_CONFIG.baseUrl}?page_size=${API_CONFIG.pageSize}&page=${page}`

    try {
      await this.writeLog('INFO', `Obteniendo página ${page} de la API`)

      const response = await axios.get(url, {
        timeout: API_CONFIG.timeout,
        headers: {
          'User-Agent': 'UpsertDebugger/1.0',
          Accept: 'application/json'
        }
      })

      if (response.data && response.data.metadata && response.data.metadata.is_error === false) {
        return {
          products: response.data.data || [],
          success: true
        }
      } else {
        throw new Error(`API error: ${response.data?.metadata?.message || 'Unknown error'}`)
      }
    } catch (error) {
      throw new Error(`API error: ${error.message}`)
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

  transformProductForDebug (product) {
    try {
      // Transformación MUY BÁSICA para debug
      const document = {
        objectID: String(product.id || product.external_id), // ⭐ CLAVE PRIMARIA
        product_id: parseInt(product.id || product.external_id),
        external_id: String(product.external_id || product.id),
        title: (product.title || '').substring(0, 500),
        title_seo: product.title_seo || this.generateTitleSeo(product.title),
        stock: product.stock || 0,
        is_active: Boolean(product.is_active),
        sale_price: product.pricing?.sales_price || product.pricing?.sale_price || 0,
        indexing_date: Math.floor(Date.now() / 1000),
        relevance_score: 50.0
      }

      return document
    } catch (error) {
      throw new Error(`Error transformando producto ${product.id}: ${error.message}`)
    }
  }

  async testUpsert () {
    console.log('\n🧪 PRUEBA DE UPSERT:')

    try {
      // 1. Obtener productos de la API
      const apiResponse = await this.fetchProductsFromAPI(1)
      const products = apiResponse.products.slice(0, 5) // Solo 5 productos para debug

      console.log(`📦 Obtenidos ${products.length} productos de la API`)

      // 2. Mostrar documentos ANTES del upsert
      const beforeDocs = await this.getExistingDocuments()
      const beforeCount = beforeDocs.length

      // 3. Transformar productos
      const documents = []
      console.log('\n🔧 TRANSFORMANDO PRODUCTOS:')
      for (const product of products) {
        const doc = this.transformProductForDebug(product)
        documents.push(doc)
        console.log(`  - Producto ${product.id} → objectID: '${doc.objectID}'`)
      }

      // 4. Verificar si ya existen documentos con estos objectIDs
      console.log('\n🔍 VERIFICANDO EXISTENCIA PREVIA:')
      for (const doc of documents) {
        const existing = beforeDocs.find(d => d.objectID === doc.objectID)
        if (existing) {
          console.log(`  ⚠️ objectID '${doc.objectID}' YA EXISTE - indexing_date: ${existing.indexing_date}`)
        } else {
          console.log(`  ✅ objectID '${doc.objectID}' es nuevo`)
        }
      }

      // 5. Ejecutar UPSERT
      console.log('\n📤 EJECUTANDO UPSERT...')
      const startTime = performance.now()

      const results = await this.client.collections(this.collectionName).documents().import(documents, {
        action: 'upsert'
      })

      const duration = (performance.now() - startTime) / 1000

      // 6. Analizar resultados del upsert
      console.log('\n📥 RESULTADOS DEL UPSERT:')
      console.log(`⏱️ Duración: ${duration.toFixed(2)}s`)
      console.log(`📊 Respuestas: ${results.length}`)

      let indexed = 0
      let failed = 0

      results.forEach((result, i) => {
        if (result.success === true) {
          indexed++
          console.log(`  ✅ ${i + 1}. ${documents[i].title?.substring(0, 40)}... → ÉXITO`)
        } else {
          failed++
          console.log(`  ❌ ${i + 1}. ${documents[i].title?.substring(0, 40)}... → ERROR: ${result.error}`)
        }
      })

      console.log(`\n📊 RESUMEN UPSERT: ${indexed} exitosos, ${failed} fallidos`)

      // 7. Verificar documentos DESPUÉS del upsert
      await this.sleep(1000) // Esperar a que Typesense procese

      const afterDocs = await this.getExistingDocuments()
      const afterCount = afterDocs.length

      console.log('\n📈 COMPARACIÓN ANTES/DESPUÉS:')
      console.log(`📦 Documentos ANTES: ${beforeCount}`)
      console.log(`📦 Documentos DESPUÉS: ${afterCount}`)
      console.log(`📊 DIFERENCIA: ${afterCount - beforeCount} documentos`)

      if (afterCount - beforeCount === indexed) {
        console.log(`✅ RESULTADO ESPERADO: Se agregaron exactamente ${indexed} documentos nuevos`)
      } else if (afterCount - beforeCount > indexed) {
        console.log(`⚠️ POSIBLE PROBLEMA: Se agregaron ${afterCount - beforeCount} pero solo ${indexed} eran exitosos`)
        console.log('💡 Esto indica que el UPSERT está creando duplicados')
      } else {
        console.log('❓ RESULTADO INESPERADO: La diferencia no coincide con los exitosos')
      }

      // 8. Verificar duplicados específicos
      console.log('\n🔍 VERIFICACIÓN DE DUPLICADOS POST-UPSERT:')
      for (const doc of documents) {
        const matches = afterDocs.filter(d => d.objectID === doc.objectID)
        if (matches.length > 1) {
          console.log(`⚠️ objectID '${doc.objectID}' tiene ${matches.length} duplicados:`)
          matches.forEach((match, i) => {
            console.log(`    ${i + 1}. indexing_date: ${match.indexing_date}`)
          })
        } else if (matches.length === 1) {
          console.log(`✅ objectID '${doc.objectID}' tiene exactamente 1 documento`)
        } else {
          console.log(`❌ objectID '${doc.objectID}' no se encontró (fallo en upsert)`)
        }
      }

      await this.writeLog('INFO', 'Prueba de upsert completada', {
        beforeCount,
        afterCount,
        difference: afterCount - beforeCount,
        indexedCount: indexed,
        failedCount: failed
      })
    } catch (error) {
      console.log(`💥 ERROR EN PRUEBA DE UPSERT: ${error.message}`)
      await this.writeLog('ERROR', 'Error en prueba de upsert', { error: error.message })
    }
  }

  sleep (ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
  }

  async run () {
    await this.init()
    await this.testUpsert()

    console.log('\n📋 DIAGNÓSTICO COMPLETADO')
    console.log(`📄 Log detallado: ${this.logFile}`)
    console.log('\n💡 PRÓXIMOS PASOS:')
    console.log('1. Revisar si objectID es consistente')
    console.log('2. Verificar configuración de campo primario en Typesense')
    console.log('3. Confirmar que el upsert está funcionando correctamente')
  }
}

// Ejecutar
async function main () {
  const indexDebug = new UpsertDebugger()
  try {
    await indexDebug.run()
  } catch (error) {
    console.error('💥 Error fatal:', error.message)
    process.exit(1)
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error)
}

export { UpsertDebugger }
