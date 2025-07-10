// scripts/typesense-diagnostic.js
import axios from 'axios'
import Typesense from 'typesense'

const TYPESENSE_CONFIG = {
  nodes: [{
    host: process.env.TYPESENSE_HOST || 'localhost',
    port: process.env.TYPESENSE_PORT || '8108',
    protocol: process.env.TYPESENSE_PROTOCOL || 'http'
  }],
  apiKey: process.env.TYPESENSE_API_KEY || 'cs-products-search-supersecret-key-2024',
  connectionTimeoutSeconds: 10,
  collectionName: process.env.TYPESENSE_COLLECTION || 'products'
}

class TypesenseDiagnostic {
  constructor () {
    this.client = new Typesense.Client(TYPESENSE_CONFIG)
    this.config = TYPESENSE_CONFIG
    this.baseUrl = `${this.config.nodes[0].protocol}://${this.config.nodes[0].host}:${this.config.nodes[0].port}`
  }

  async runFullDiagnostic () {
    console.log('🔍 DIAGNÓSTICO COMPLETO DE TYPESENSE')
    console.log('='.repeat(50))

    await this.checkEnvironment()
    await this.checkConnection()
    await this.checkServerInfo()
    await this.checkCollections()
    await this.testImportFormat()
    await this.testSingleDocumentInsert()

    console.log('\n✅ Diagnóstico completado')
  }

  async checkEnvironment () {
    console.log('\n1️⃣ VERIFICANDO CONFIGURACIÓN:')
    console.log(`📍 Host: ${this.config.nodes[0].host}`)
    console.log(`🔌 Puerto: ${this.config.nodes[0].port}`)
    console.log(`🔐 Protocolo: ${this.config.nodes[0].protocol}`)
    console.log(`🔑 API Key: ${this.config.apiKey.substring(0, 10)}...`)
    console.log(`📂 Colección: ${this.config.collectionName}`)
    console.log(`🌐 URL completa: ${this.baseUrl}`)
  }

  async checkConnection () {
    console.log('\n2️⃣ VERIFICANDO CONEXIÓN:')

    try {
      // Test con cliente oficial
      console.log('🔄 Probando con cliente oficial...')
      const health = await this.client.health.retrieve()
      console.log(`✅ Cliente oficial: ${JSON.stringify(health)}`)
    } catch (error) {
      console.log(`❌ Cliente oficial falló: ${error.message}`)
    }

    try {
      // Test con HTTP directo
      console.log('🔄 Probando con HTTP directo...')
      const response = await axios.get(`${this.baseUrl}/health`, {
        headers: { 'X-TYPESENSE-API-KEY': this.config.apiKey },
        timeout: 5000
      })
      console.log(`✅ HTTP directo: ${JSON.stringify(response.data)}`)
    } catch (error) {
      console.log(`❌ HTTP directo falló: ${error.message}`)

      if (error.code === 'ECONNREFUSED') {
        console.log('💡 Typesense no está ejecutándose o no es accesible en el puerto especificado')
      } else if (error.response?.status === 401) {
        console.log('💡 Error de autenticación - verifica tu API key')
      }
    }
  }

  async checkServerInfo () {
    console.log('\n3️⃣ INFORMACIÓN DEL SERVIDOR:')

    try {
      const response = await axios.get(`${this.baseUrl}/debug`, {
        headers: { 'X-TYPESENSE-API-KEY': this.config.apiKey }
      })

      console.log(`📊 Versión: ${response.data.version}`)
      console.log(`💾 Estado: ${response.data.state}`)
    } catch (error) {
      console.log(`⚠️ No se pudo obtener info del servidor: ${error.message}`)
    }
  }

  async checkCollections () {
    console.log('\n4️⃣ VERIFICANDO COLECCIONES:')

    try {
      // Listar todas las colecciones
      const collections = await this.client.collections().retrieve()
      console.log(`📁 Colecciones encontradas: ${collections.length}`)

      collections.forEach(col => {
        console.log(`  - ${col.name}: ${col.num_documents} documentos`)
      })

      // Verificar colección específica
      try {
        const collection = await this.client.collections(this.config.collectionName).retrieve()
        console.log(`\n📋 Detalles de la colección '${this.config.collectionName}':`)
        console.log(`  📦 Documentos: ${collection.num_documents}`)
        console.log(`  📋 Campos: ${collection.fields?.length || 0}`)
        console.log(`  🎯 Campo de ordenamiento: ${collection.default_sorting_field || 'ninguno'}`)

        if (collection.fields) {
          const facetFields = collection.fields.filter(f => f.facet).map(f => f.name)
          console.log(`  🏷️ Campos con facetas: ${facetFields.join(', ') || 'ninguno'}`)
        }
      } catch (error) {
        console.log(`❌ La colección '${this.config.collectionName}' no existe`)
      }
    } catch (error) {
      console.log(`❌ Error obteniendo colecciones: ${error.message}`)
    }
  }

  async testImportFormat () {
    console.log('\n5️⃣ PROBANDO FORMATO DE IMPORT:')

    // Crear documento de prueba
    const testDoc = {
      objectID: 'test-diagnostic-' + Date.now(),
      product_id: 999999,
      external_id: 'test-999999',
      title: 'Producto de Prueba Diagnóstico',
      price: 100.00,
      sale_price: 90.00,
      stock: 1,
      is_active: true,
      relevance_score: 50.0,
      indexing_date: Math.floor(Date.now() / 1000),
      division: 1
    }

    try {
      // Asegurar que la colección existe
      await this.ensureTestCollection()

      console.log('🔄 Probando import con cliente oficial...')
      const result = await this.client.collections(this.config.collectionName).documents().import([testDoc], {
        action: 'upsert'
      })

      console.log(`✅ Import exitoso: ${JSON.stringify(result)}`)

      // Verificar que se insertó
      try {
        const retrieved = await this.client.collections(this.config.collectionName).documents(testDoc.objectID).retrieve()
        console.log(`✅ Documento recuperado exitosamente: ${retrieved.title}`)

        // Limpiar
        await this.client.collections(this.config.collectionName).documents(testDoc.objectID).delete()
        console.log('🧹 Documento de prueba eliminado')
      } catch (error) {
        console.log(`⚠️ No se pudo recuperar el documento: ${error.message}`)
      }
    } catch (error) {
      console.log(`❌ Error en import: ${error.message}`)

      // Información adicional sobre el error
      if (error.httpStatus) {
        console.log(`📊 HTTP Status: ${error.httpStatus}`)
      }
      if (error.importResults) {
        console.log(`📋 Resultados de import: ${JSON.stringify(error.importResults)}`)
      }
    }
  }

  async testSingleDocumentInsert () {
    console.log('\n6️⃣ PROBANDO INSERCIÓN INDIVIDUAL:')

    const testDoc = {
      objectID: 'test-single-' + Date.now(),
      product_id: 888888,
      external_id: 'test-888888',
      title: 'Producto Individual de Prueba',
      price: 200.00,
      sale_price: 180.00,
      stock: 1,
      is_active: true,
      relevance_score: 75.0,
      indexing_date: Math.floor(Date.now() / 1000),
      division: 1
    }

    try {
      await this.ensureTestCollection()

      console.log('🔄 Insertando documento individual...')
      const result = await this.client.collections(this.config.collectionName).documents().create(testDoc)
      console.log(`✅ Inserción exitosa: ${JSON.stringify(result)}`)

      // Limpiar
      await this.client.collections(this.config.collectionName).documents(testDoc.objectID).delete()
      console.log('🧹 Documento individual eliminado')
    } catch (error) {
      console.log(`❌ Error en inserción individual: ${error.message}`)
    }
  }

  async ensureTestCollection () {
    try {
      await this.client.collections(this.config.collectionName).retrieve()
    } catch (error) {
      if (error.httpStatus === 404) {
        console.log('📁 Creando colección de prueba...')

        const schema = {
          name: this.config.collectionName,
          fields: [
            { name: 'objectID', type: 'string' },
            { name: 'product_id', type: 'int32' },
            { name: 'external_id', type: 'string' },
            { name: 'title', type: 'string' },
            { name: 'price', type: 'float' },
            { name: 'sale_price', type: 'float' },
            { name: 'stock', type: 'int32' },
            { name: 'is_active', type: 'bool' },
            { name: 'relevance_score', type: 'float' },
            { name: 'indexing_date', type: 'int64' },
            { name: 'division', type: 'int32' }
          ],
          default_sorting_field: 'relevance_score'
        }

        await this.client.collections().create(schema)
        console.log('✅ Colección de prueba creada')
      } else {
        throw error
      }
    }
  }

  async testSpecificError () {
    console.log('\n🔧 PROBANDO EL ERROR ESPECÍFICO:')

    try {
      // Simular exactamente lo que hace el indexer original
      const testDocs = [
        {
          objectID: 'test-1',
          product_id: 1,
          external_id: 'test-1',
          title: 'Test Product 1',
          price: 100,
          sale_price: 90,
          stock: 5,
          is_active: true,
          relevance_score: 50,
          indexing_date: Math.floor(Date.now() / 1000),
          division: 1
        },
        {
          objectID: 'test-2',
          product_id: 2,
          external_id: 'test-2',
          title: 'Test Product 2',
          price: 200,
          sale_price: 180,
          stock: 3,
          is_active: true,
          relevance_score: 60,
          indexing_date: Math.floor(Date.now() / 1000),
          division: 1
        }
      ]

      await this.ensureTestCollection()

      // Probar con HTTP directo (como en el indexer original)
      console.log('🔄 Probando con HTTP directo (JSONL)...')
      const importData = testDocs.map(doc => JSON.stringify(doc)).join('\n')

      const response = await axios.post(
        `${this.baseUrl}/collections/${this.config.collectionName}/documents/import?action=upsert`,
        importData,
        {
          headers: {
            'X-TYPESENSE-API-KEY': this.config.apiKey,
            'Content-Type': 'application/jsonl'
          },
          timeout: 10000
        }
      )

      console.log(`📊 Tipo de respuesta: ${typeof response.data}`)
      console.log(`📊 Es array: ${Array.isArray(response.data)}`)
      console.log(`📊 Respuesta: ${JSON.stringify(response.data).substring(0, 300)}...`)

      // Intentar procesar como en el indexer original
      if (typeof response.data === 'string') {
        const results = response.data.split('\n').filter(line => line.trim())
        console.log(`✅ Procesado como string: ${results.length} resultados`)
        results.forEach((result, i) => {
          try {
            const parsed = JSON.parse(result)
            console.log(`  ${i + 1}: ${parsed.success ? 'éxito' : 'fallo'}`)
          } catch (e) {
            console.log(`  ${i + 1}: error parseando - ${result}`)
          }
        })
      } else if (Array.isArray(response.data)) {
        console.log(`✅ Procesado como array: ${response.data.length} resultados`)
        response.data.forEach((result, i) => {
          console.log(`  ${i + 1}: ${result.success ? 'éxito' : 'fallo'}`)
        })
      }

      // Limpiar documentos de prueba
      for (const doc of testDocs) {
        try {
          await this.client.collections(this.config.collectionName).documents(doc.objectID).delete()
        } catch (e) {
          // Ignorar errores de limpieza
        }
      }
    } catch (error) {
      console.log(`❌ Error específico: ${error.message}`)
      if (error.response?.data) {
        console.log(`📊 Respuesta de error: ${JSON.stringify(error.response.data)}`)
      }
    }
  }
}

// Ejecutar diagnóstico
async function main () {
  const diagnostic = new TypesenseDiagnostic()

  try {
    await diagnostic.runFullDiagnostic()
    await diagnostic.testSpecificError()
  } catch (error) {
    console.error('💥 Error en diagnóstico:', error.message)
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error)
}

export { TypesenseDiagnostic }
