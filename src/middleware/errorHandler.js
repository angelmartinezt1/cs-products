export function errorHandler(error, req, res, next) {
  console.error('Error:', error)
  
  // Error de base de datos
  if (error.code && error.code.startsWith('ER_')) {
    return res.status(500).json({
      error: 'Database error',
      message: 'An error occurred while processing your request'
    })
  }
  
  // Error de validación
  if (error.name === 'ValidationError') {
    return res.status(400).json({
      error: 'Validation error',
      message: error.message
    })
  }
  
  // Error genérico
  res.status(500).json({
    error: 'Internal server error',
    message: process.env.NODE_ENV === 'development' ? error.message : 'Something went wrong'
  })
}