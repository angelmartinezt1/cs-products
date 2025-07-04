export function validateSearchParams(req, res, next) {
  const { page, limit, sort, order } = req.query
  
  // Validar page
  if (page && (isNaN(page) || parseInt(page) < 1)) {
    return res.status(400).json({ 
      error: 'Invalid page parameter. Must be a positive integer.' 
    })
  }
  
  // Validar limit
  if (limit && (isNaN(limit) || parseInt(limit) < 1 || parseInt(limit) > 100)) {
    return res.status(400).json({ 
      error: 'Invalid limit parameter. Must be between 1 and 100.' 
    })
  }
  
  // Validar sort
  const validSorts = ['relevance', 'price', 'rating', 'newest', 'name', 'reviews', 'discount']
  if (sort && !validSorts.includes(sort)) {
    return res.status(400).json({ 
      error: `Invalid sort parameter. Must be one of: ${validSorts.join(', ')}` 
    })
  }
  
  // Validar order
  const validOrders = ['asc', 'desc']
  if (order && !validOrders.includes(order.toLowerCase())) {
    return res.status(400).json({ 
      error: `Invalid order parameter. Must be 'asc' or 'desc'` 
    })
  }
  
  next()
}