// scripts/ultra-simple-test.js
import mysql from 'mysql2/promise'
import { ProductIndexer } from './product-indexer.js'

// Servicio ultra simple integrado en el script
class UltraSimpleService {
  constructor (connection) {
    this.connection = connection
  }

  async insertProductsUltraSimple (products) {
    let inserted = 0
    let updated = 0
    let errors = 0

    console.log(`📦 Procesando ${products.length} productos uno por uno (ultra simple)...`)

    for (const product of products) {
      try {
        // Verificar si existe
        const [existing] = await this.connection.execute(
          'SELECT id FROM products WHERE id = ?',
          [product.id]
        )

        if (existing.length > 0) {
          // UPDATE simple
          await this.connection.execute(`
            UPDATE products SET 
              name = ?, sales_price = ?, stock = ?, status = ?, 
              category_path = ?, brand = ?, updated_at = CURRENT_TIMESTAMP 
            WHERE id = ?
          `, [
            product.name, product.sales_price, product.stock, product.status,
            product.category_path, product.brand, product.id
          ])
          updated++
        } else {
          // INSERT simple
          await this.connection.execute(`
            INSERT INTO products (
              id, name, sales_price, stock, status, visible,
              category_path, brand, category_lvl0, category_lvl1, category_lvl2
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `, [
            product.id, product.name, product.sales_price, product.stock, product.status, 1,
            product.category_path, product.brand, product.category_lvl0, product.category_lvl1, product.category_lvl2
          ])
          inserted++
        }
      } catch (error) {
        console.warn(`⚠️ Error con producto ${product.id}: ${error.message}`)
        errors++
      }
    }

    return { inserted, updated, errors }
  }

  async getStats () {
    const [stats] = await this.connection.execute(`
      SELECT 
        COUNT(*) as total_products,
        COUNT(CASE WHEN status = 1 THEN 1 END) as active_products
      FROM products
    `)
    return stats[0]
  }
}

async function ultraSimpleTest () {
  console.log('🧪 Test ULTRA SIMPLE - Evitando todos los problemas MySQL...')

  let connection = null

  try {
    // 1. Conexión básica
    console.log('🔌 Conectando...')
    connection = await mysql.createConnection({
      host: process.env.DB_HOST || 'localhost',
      user: process.env.DB_USER || 'root',
      password: process.env.DB_PASSWORD || '',
      database: process.env.DB_NAME || 'search_products',
      charset: 'utf8mb4'
    })
    console.log('✅ Conectado')

    // 2. Servicios
    const indexer = new ProductIndexer()
    indexer.connection = connection

    const service = new UltraSimpleService(connection)

    // 3. API
    console.log('📡 Obteniendo productos...')
    const apiResponse = await indexer.fetchProductsFromAPI(1)

    if (!apiResponse.success) {
      throw new Error('API falló')
    }

    console.log(`✅ ${apiResponse.products.length} productos obtenidos`)

    // 4. Transformar solo 5 productos para test
    console.log('🔄 Transformando 5 productos...')
    const sampleProducts = apiResponse.products.slice(0, 5)
    const transformedProducts = sampleProducts.map(p => indexer.transformProduct(p))

    // Debug del primer producto
    console.log('\n🔍 Producto ejemplo:')
    const firstProduct = transformedProducts[0]
    console.log('- ID:', firstProduct.id)
    console.log('- Name:', firstProduct.name?.substring(0, 50))
    console.log('- Price:', firstProduct.sales_price)
    console.log('- Category Path:', firstProduct.category_path)

    // 5. Insertar usando método ULTRA SIMPLE
    console.log('\n💾 Insertando con método ultra simple...')
    const result = await service.insertProductsUltraSimple(transformedProducts)

    console.log('\n📊 RESULTADO FINAL:')
    console.log(`✅ Insertados: ${result.inserted}`)
    console.log(`🔄 Actualizados: ${result.updated}`)
    console.log(`❌ Errores: ${result.errors}`)

    // 6. Estadísticas
    const stats = await service.getStats()
    console.log(`📦 Total en BD: ${stats.total_products}`)
    console.log(`✅ Activos: ${stats.active_products}`)

    console.log('\n🎉 Test ULTRA SIMPLE completado!')
  } catch (error) {
    console.error('💥 Error:', error.message)
    console.error('Stack:', error.stack)
  } finally {
    if (connection) {
      await connection.end()
      console.log('🔌 Desconectado')
    }
  }
}

ultraSimpleTest().catch(console.error)
