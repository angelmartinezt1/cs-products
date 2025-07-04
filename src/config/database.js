import mysql from 'mysql2/promise'

// Pool de conexiones para mejor rendimiento
const pool = mysql.createPool({
  host: process.env.DB_HOST || 'localhost',
  port: process.env.DB_PORT || 3306,
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME || 'cs_products',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  enableKeepAlive: true,
  keepAliveInitialDelay: 0,
  // Configuraciones específicas para búsquedas
  dateStrings: true,
  supportBigNumbers: true,
  bigNumberStrings: true,
  // Configuración adicional para evitar problemas
  charset: 'utf8mb4',
  timezone: '+00:00'
})

// Función helper para ejecutar queries - USAR QUERY EN LUGAR DE EXECUTE
export async function executeQuery(sql, params = []) {
  try {
    console.log('🔍 Executing SQL:', sql.trim())
    console.log('📊 With params:', params)
    
    // Usar query() en lugar de execute() para evitar problemas
    const [rows] = await pool.query(sql, params)
    
    console.log('✅ Query executed successfully, rows:', rows.length)
    return rows
  } catch (error) {
    console.error('❌ Database query error:', error)
    console.error('SQL:', sql)
    console.error('Params:', params)
    throw new Error(`Database query failed: ${error.message}`)
  }
}

// Función para transacciones
export async function executeTransaction(queries) {
  const connection = await pool.getConnection()
  try {
    await connection.beginTransaction()
    
    const results = []
    for (const { sql, params } of queries) {
      const [result] = await connection.query(sql, params) // También cambiar aquí
      results.push(result)
    }
    
    await connection.commit()
    return results
  } catch (error) {
    await connection.rollback()
    throw error
  } finally {
    connection.release()
  }
}

// Test de conexión
export async function testConnection() {
  try {
    const [rows] = await pool.query('SELECT 1 as test')
    console.log('✅ Database connection successful')
    return true
  } catch (error) {
    console.error('❌ Database connection failed:', error.message)
    return false
  }
}

// Cerrar pool al terminar la aplicación
process.on('SIGINT', async () => {
  console.log('Closing database pool...')
  await pool.end()
  process.exit(0)
})

export { pool }
