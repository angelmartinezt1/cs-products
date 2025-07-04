import cors from 'cors'
import express from 'express'
import { errorHandler } from './middleware/errorHandler.js'
import { requestLogger } from './middleware/requestLogger.js'
import { categoriesRoutes } from './routes/categories.js'
import { facetsRoutes } from './routes/facets.js'
import { searchRoutes } from './routes/search.js'

const app = express()
const PORT = process.env.PORT || 3005  // También actualiza aquí el puerto

// Middlewares
app.use(cors())
app.use(express.json())
app.use(requestLogger)

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    service: 'cs-products-search'
  })
})

// Routes
app.use('/api/search', searchRoutes)
app.use('/api/facets', facetsRoutes)
app.use('/api/categories', categoriesRoutes)

// 404 handler - SIN el asterisco
app.use((req, res) => {
  res.status(404).json({
    error: 'Not Found',
    message: `Route ${req.originalUrl} not found`
  })
})

// Error handler
app.use(errorHandler)

app.listen(PORT, () => {
  console.log(`🚀 CS Products Search API running on port ${PORT}`)
  console.log(`📍 Health check: http://localhost:${PORT}/health`)
  console.log(`🔍 Search endpoint: http://localhost:${PORT}/api/search`)
})