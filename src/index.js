// src/index.js
import cors from 'cors'
import express from 'express'
import { errorHandler } from './middleware/errorHandler.js'
import { requestLogger } from './middleware/requestLogger.js'
import { algoliaRoutes } from './routes/algolia.js'
import { categoriesRoutes } from './routes/categories.js'
import { facetsRoutes } from './routes/facets.js'
import { searchRoutes } from './routes/search.js'

const app = express()
const PORT = process.env.PORT || 3005

// Middlewares
app.use(cors({
  origin: ['http://localhost:3000', 'http://localhost:3005'],
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'x-algolia-agent', 'x-algolia-api-key', 'x-algolia-application-id']
}))

app.use(express.json())
app.use(express.urlencoded({ extended: true })) // Para compatibilidad con form-data de Algolia
app.use(requestLogger)

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    service: 'cs-products-search'
  })
})

// Routes nativas
app.use('/api/search', searchRoutes)
app.use('/api/facets', facetsRoutes)
app.use('/api/categories', categoriesRoutes)

// Routes compatibles con Algolia
app.use('/', algoliaRoutes) // Sin prefijo para que /1/indexes/*/queries funcione

// Ruta adicional para compatibilidad completa con el formato de Algolia
app.post('/1/indexes/:indexName/queries', async (req, res, next) => {
  // Redireccionar a la ruta principal de Algolia
  req.url = '/1/indexes/*/queries'
  algoliaRoutes(req, res, next)
})

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    error: 'Not Found',
    message: `Route ${req.originalUrl} not found`,
    availableEndpoints: [
      'GET /health',
      'GET /api/search',
      'POST /api/search/batch',
      'GET /api/facets',
      'GET /api/categories',
      'POST /1/indexes/*/queries (Algolia compatible)'
    ]
  })
})

// Error handler
app.use(errorHandler)

app.listen(PORT, () => {
  console.log(`🚀 CS Products Search API running on port ${PORT}`)
  console.log(`📍 Health check: http://localhost:${PORT}/health`)
  console.log(`🔍 Search endpoint: http://localhost:${PORT}/api/search`)
  console.log(`🔄 Algolia compatible: http://localhost:${PORT}/1/indexes/*/queries`)
  console.log(`🏷️  Facets endpoint: http://localhost:${PORT}/api/facets`)
  console.log(`📁 Categories endpoint: http://localhost:${PORT}/api/categories`)
})

export default app