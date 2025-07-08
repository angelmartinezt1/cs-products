// scripts/debug-exact-error.js
import mysql from 'mysql2/promise'
import { ProductIndexer } from './product-indexer.js'

async function debugExactError () {
  console.log('🔍 DEBUG: Encontrando la línea exacta que causa el error...')

  let connection = null

  try {
    // 1. Conexión
    console.log('🔌 Conectando...')
    connection = await mysql.createConnection({
      host: process.env.DB_HOST || 'localhost',
      user: process.env.DB_USER || 'root',
      password: process.env.DB_PASSWORD || '',
      database: process.env.DB_NAME || 'search_products',
      charset: 'utf8mb4'
    })
    console.log('✅ Conectado')

    // 2. Obtener 1 producto de la API
    console.log('📡 Obteniendo 1 producto...')
    const indexer = new ProductIndexer()
    indexer.connection = connection

    const apiResponse = await indexer.fetchProductsFromAPI(1)
    const product = apiResponse.products[0]
    const transformedProduct = indexer.transformProduct(product)

    console.log('✅ Producto transformado:', {
      id: transformedProduct.id,
      name: transformedProduct.name?.substring(0, 30) + '...'
    })

    // 3. Test paso a paso - cada query individual
    console.log('\n🧪 TEST 1: SELECT simple')
    try {
      const [existing] = await connection.execute(
        'SELECT id FROM products WHERE id = ?',
        [transformedProduct.id]
      )
      console.log('✅ SELECT funciona:', existing.length > 0 ? 'Producto existe' : 'Producto nuevo')
    } catch (error) {
      console.error('❌ SELECT falló:', error.message)
      return
    }

    // 4. Test INSERT simple
    console.log('\n🧪 TEST 2: INSERT simple')
    try {
      await connection.execute(`
        INSERT INTO products (id, name, sales_price, stock, status, visible, category_path, brand) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        999999, 'TEST PRODUCT', 100, 1, 1, 1, 'test > category', 'TEST BRAND'
      ])
      console.log('✅ INSERT simple funciona')

      // Limpiar
      await connection.execute('DELETE FROM products WHERE id = 999999')
    } catch (error) {
      console.error('❌ INSERT simple falló:', error.message)
    }

    // 5. Test UPDATE simple
    console.log('\n🧪 TEST 3: UPDATE simple')
    try {
      // Insertar primero
      await connection.execute(`
        INSERT INTO products (id, name, sales_price) VALUES (999998, 'TEST', 50)
      `)

      // Luego actualizar
      await connection.execute(`
        UPDATE products SET name = ?, sales_price = ? WHERE id = ?
      `, ['UPDATED TEST', 75, 999998])

      console.log('✅ UPDATE simple funciona')

      // Limpiar
      await connection.execute('DELETE FROM products WHERE id = 999998')
    } catch (error) {
      console.error('❌ UPDATE simple falló:', error.message)
    }

    // 6. Test con el producto real (INSERT)
    console.log('\n🧪 TEST 4: INSERT con producto real')
    try {
      await connection.execute(`
        INSERT INTO products (
          id, name, sales_price, stock, status, visible,
          category_path, brand, category_lvl0, category_lvl1, category_lvl2
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        transformedProduct.id + 100000, // ID único para evitar conflictos
        transformedProduct.name,
        transformedProduct.sales_price,
        transformedProduct.stock,
        transformedProduct.status,
        1,
        transformedProduct.category_path,
        transformedProduct.brand,
        transformedProduct.category_lvl0,
        transformedProduct.category_lvl1,
        transformedProduct.category_lvl2
      ])
      console.log('✅ INSERT con producto real funciona')

      // Limpiar
      await connection.execute('DELETE FROM products WHERE id = ?', [transformedProduct.id + 100000])
    } catch (error) {
      console.error('❌ INSERT con producto real falló:', error.message)
      console.error('Producto que falló:', {
        id: transformedProduct.id,
        name: transformedProduct.name,
        sales_price: transformedProduct.sales_price,
        brand: transformedProduct.brand,
        category_path: transformedProduct.category_path
      })
    }

    // 7. Test con transacción
    console.log('\n🧪 TEST 5: Con transacción')
    try {
      await connection.execute('START TRANSACTION')

      await connection.execute(`
        INSERT INTO products (id, name, sales_price) VALUES (?, ?, ?)
      `, [999997, 'TRANSACTION TEST', 25])

      await connection.execute('COMMIT')
      console.log('✅ Transacción funciona')

      // Limpiar
      await connection.execute('DELETE FROM products WHERE id = 999997')
    } catch (error) {
      await connection.execute('ROLLBACK')
      console.error('❌ Transacción falló:', error.message)
    }

    // 8. Test completo simulando UltraSimpleBulkService
    console.log('\n🧪 TEST 6: Simulando UltraSimpleBulkService completo')
    try {
      // El método exacto que usa UltraSimpleBulkService
      const [existing2] = await connection.execute(
        'SELECT id FROM products WHERE id = ?',
        [transformedProduct.id]
      )

      if (existing2.length > 0) {
        console.log('Producto existe, haciendo UPDATE...')
        await connection.execute(`
          UPDATE products SET 
            name = ?, sales_price = ?, stock = ?, status = ?, 
            category_path = ?, brand = ?, updated_at = CURRENT_TIMESTAMP 
          WHERE id = ?
        `, [
          transformedProduct.name,
          transformedProduct.sales_price,
          transformedProduct.stock,
          transformedProduct.status,
          transformedProduct.category_path,
          transformedProduct.brand,
          transformedProduct.id
        ])
        console.log('✅ UPDATE completo funciona')
      } else {
        console.log('Producto nuevo, haciendo INSERT...')
        await connection.execute(`
          INSERT INTO products (
            id, name, sales_price, stock, status, visible,
            category_path, brand, category_lvl0, category_lvl1, category_lvl2
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `, [
          transformedProduct.id,
          transformedProduct.name,
          transformedProduct.sales_price,
          transformedProduct.stock,
          transformedProduct.status,
          1,
          transformedProduct.category_path,
          transformedProduct.brand,
          transformedProduct.category_lvl0,
          transformedProduct.category_lvl1,
          transformedProduct.category_lvl2
        ])
        console.log('✅ INSERT completo funciona')
      }
    } catch (error) {
      console.error('❌ Simulación UltraSimpleBulkService falló:', error.message)
      console.error('Error stack:', error.stack)
    }

    console.log('\n🎯 DEBUG COMPLETADO')
  } catch (error) {
    console.error('💥 Error general en debug:', error.message)
    console.error('Stack:', error.stack)
  } finally {
    if (connection) {
      await connection.end()
      console.log('🔌 Desconectado')
    }
  }
}

debugExactError().catch(console.error)
