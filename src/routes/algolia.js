// src/routes/algolia.js
import express from 'express'
import { FacetService } from '../services/facetService.js'
import { SearchService } from '../services/searchService.js'

const router = express.Router()

router.post('/1/indexes/queries', async (req, res, next) => {
  try {
    const { requests } = req.body
    
    if (!Array.isArray(requests)) {
      return res.status(400).json({
        message: 'requests must be an array',
        status: 400
      })
    }

    // Procesar cada request en paralelo
    const results = await Promise.all(
      requests.map(async (request, index) => {
        try {
          return await processAlgoliaRequest(request, index)
        } catch (error) {
          console.error(`Error processing request ${index}:`, error)
          return {
            hits: [],
            nbHits: 0,
            page: 0,
            nbPages: 0,
            hitsPerPage: 20,
            facets: {},
            facets_stats: {},
            exhaustiveFacetsCount: true,
            exhaustiveNbHits: true,
            exhaustiveTypo: true,
            exhaustive: {
              facetsCount: true,
              nbHits: true,
              typo: true
            },
            query: request.params?.query || '',
            params: request.params || '',
            index: request.indexName || 'sears',
            processingTimeMS: 1,
            processingTimingsMS: {
              total: 1
            },
            serverTimeMS: 1
          }
        }
      })
    )

    res.json({ results })
  } catch (error) {
    next(error)
  }
})

/**
 * Procesar un request individual de Algolia
 */
async function processAlgoliaRequest(request) {
  const { indexName, params } = request
  
  // Parsear parámetros de Algolia
  const algoliaParams = parseAlgoliaParams(params)
  
  // Solo procesar si hitsPerPage > 0 (queries de datos), sino solo facetas
  if (algoliaParams.hitsPerPage === 0) {
    return await processFacetOnlyRequest(algoliaParams)
  }
  
  // Convertir parámetros de Algolia a nuestro formato
  const searchParams = convertAlgoliaToOurFormat(algoliaParams)
  
  // Ejecutar búsqueda
  const searchResults = await SearchService.searchProducts(searchParams)
  
  // Convertir resultado a formato Algolia
  return convertToAlgoliaFormat(searchResults, algoliaParams)
}

/**
 * Procesar request que solo requiere facetas (hitsPerPage = 0)
 */
async function processFacetOnlyRequest(algoliaParams) {
  const filters = extractFiltersFromAlgolia(algoliaParams)
  
  let facets = {}
  let nbHits = 0
  
  // Si hay facets solicitadas, obtenerlas
  if (algoliaParams.facets && algoliaParams.facets.length > 0) {
    try {
      const facetResults = await FacetService.getFacets('', filters)
      facets = convertFacetsToAlgoliaFormat(facetResults, algoliaParams.facets)
      
      // Estimar total de hits para facetas
      if (Object.keys(facets).length > 0) {
        const firstFacet = Object.values(facets)[0]
        if (typeof firstFacet === 'object') {
          nbHits = Object.values(firstFacet).reduce((sum, count) => sum + count, 0)
        }
      }
    } catch (error) {
      console.error('Error getting facets:', error)
    }
  }
  
  return {
    hits: [],
    nbHits,
    page: 0,
    nbPages: 0,
    hitsPerPage: 0,
    facets,
    facets_stats: calculateFacetStats(facets),
    exhaustiveFacetsCount: true,
    exhaustiveNbHits: true,
    exhaustiveTypo: true,
    exhaustive: {
      facetsCount: true,
      nbHits: true,
      typo: true
    },
    query: algoliaParams.query || '',
    params: algoliaParams.originalParams || '',
    index: 'sears',
    processingTimeMS: 5,
    processingTimingsMS: {
      total: 5
    },
    serverTimeMS: 10
  }
}

/**
 * Parsear parámetros de URL de Algolia
 */
function parseAlgoliaParams(paramsString) {
  if (!paramsString) return {}
  
  const params = new URLSearchParams(paramsString)
  const parsed = {
    originalParams: paramsString
  }
  
  // Parámetros básicos
  parsed.query = params.get('query') || ''
  parsed.page = parseInt(params.get('page')) || 0
  parsed.hitsPerPage = parseInt(params.get('hitsPerPage')) || 20
  
  // Facetas solicitadas
  if (params.get('facets')) {
    try {
      parsed.facets = JSON.parse(decodeURIComponent(params.get('facets')))
    } catch {
      parsed.facets = []
    }
  }
  
  // Filtros de facetas
  if (params.get('facetFilters')) {
    try {
      parsed.facetFilters = JSON.parse(decodeURIComponent(params.get('facetFilters')))
    } catch {
      parsed.facetFilters = []
    }
  }
  
  // Otros parámetros
  parsed.analytics = params.get('analytics') === 'true'
  parsed.clickAnalytics = params.get('clickAnalytics') === 'true'
  
  return parsed
}

/**
 * Convertir parámetros de Algolia a nuestro formato
 */
function convertAlgoliaToOurFormat(algoliaParams) {
  const filters = extractFiltersFromAlgolia(algoliaParams)

  console.log('🔧 Filters being passed to SearchService:', filters);

  
  return {
    query: algoliaParams.query || '',
    page: algoliaParams.page + 1, // Algolia usa base 0, nosotros base 1
    limit: algoliaParams.hitsPerPage || 20,
    sortBy: 'relevance',
    sortOrder: 'desc',
    facets: algoliaParams.facets && algoliaParams.facets.length > 0,
    filters
  }
}

/**
 * Extraer filtros de los parámetros de Algolia
 */
function extractFiltersFromAlgolia(algoliaParams) {
  const filters = {}
  
  if (!algoliaParams.facetFilters) return filters
  
  // Procesar facetFilters de Algolia
  algoliaParams.facetFilters.forEach(filterGroup => {
    if (Array.isArray(filterGroup)) {
      // OR group - tomar el primero por simplicidad
      if (filterGroup.length > 0) {
        parseFilter(filterGroup[0], filters)
      }
    } else {
      // Filtro simple
      parseFilter(filterGroup, filters)
    }
  })
  
  return filters
}

/**
 * Parsear un filtro individual
 */
function parseFilter(filterString, filters) {
  if (!filterString || typeof filterString !== 'string') return
  
  // Manejar diferentes formatos de filtros
  if (filterString.includes(':')) {
    const [key, value] = filterString.split(':')
    
    switch (key) {
      case 'hirerarchical_category.lvl0':
        filters.category_lvl0 = value
        break
      case 'hirerarchical_category.lvl1':
        filters.category_lvl1 = value
        break
      case 'hirerarchical_category.lvl2':
        filters.category_lvl2 = value
        break
      case 'brand':
        filters.brand = value
        break
      case 'fulfillment':
        filters.fulfillment_type = value === 'true' ? 'fulfillment' : 'seller'
        break
      case 'has_free_shipping':
        filters.free_shipping = value === 'true'
        break
      default:
        // Manejar otros filtros genéricamente
        filters[key] = value
    }
  }
}

/**
 * Convertir resultado a formato Algolia
 */
function convertToAlgoliaFormat(searchResults, algoliaParams) {
  // 🔍 DEBUGGING TEMPORAL - Agregar estos logs
  console.log('🔍 DEBUG - SearchResults facets:', searchResults.facets);
  console.log('🔍 DEBUG - Requested facets:', algoliaParams.facets);
  if (searchResults.facets) {
    console.log('🔍 DEBUG - Facets keys:', Object.keys(searchResults.facets));
  }

  const hits = searchResults.products.map(product => {
    // DEBUGGING: Log the category values to see what we're getting
    console.log(`Product ${product.id} categories:`, {
      category_lvl0: product.category_lvl0,
      category_lvl1: product.category_lvl1, 
      category_lvl2: product.category_lvl2,
      category_path: product.category_path
    });

    // Parse category path if individual levels are missing
    let lvl0 = product.category_lvl0;
    let lvl1 = product.category_lvl1;
    let lvl2 = product.category_lvl2;

    // FALLBACK: If category levels are null but we have category_path, parse it
    if ((!lvl0 || !lvl1 || !lvl2) && product.category_path) {
      const pathParts = product.category_path.split(' > ').map(part => part.trim());
      if (pathParts.length >= 1 && !lvl0) lvl0 = pathParts[0];
      if (pathParts.length >= 2 && !lvl1) lvl1 = pathParts.slice(0, 2).join(' > ');
      if (pathParts.length >= 3 && !lvl2) lvl2 = pathParts.join(' > ');
    }

    // FALLBACK: Use category_name as last resort
    if (!lvl0 && product.category_name) {
      lvl0 = product.category_name;
    }

    return {
      objectID: product.id.toString(),
      title: product.name,
      ean: product.sku,
      sku: product.sku,
      brand: product.brand,
      price: product.sales_price,
      sale_price: product.sales_price,
      stock: product.stock,
      is_active: product.status === 1,
      relevance_amount: Math.floor(Math.random() * 10000),
      relevance_sales: Math.floor(Math.random() * 20),
      percent_off: product.percentage_discount,
      description: product.description,
      fulfillment: product.fulfillment_type === 'fulfillment',
      has_free_shipping: product.has_free_shipping === 1,
      store_only: product.is_store_only === 1,
      store_pickup: product.is_store_pickup === 1,
      photos: product.main_image ? [{
        id: Math.floor(Math.random() * 1000000),
        source: product.main_image,
        thumbnail: product.thumbnail || product.main_image
      }] : [],
      attributes: {},
      categories: {
        [product.category_id]: product.category_name
      },
      // FIXED: Better handling of hierarchical categories
      hirerarchical_category: {
        lvl0: lvl0 ? [lvl0] : [],
        lvl1: lvl1 ? [lvl1] : [],
        lvl2: lvl2 ? [lvl2] : []
      },
      sellers: [{
        id: product.store_id,
        name: product.store_name,
        store_rating: product.store_rating
      }],
      review_rating: product.review_rating || 0,
      total_reviews: product.total_reviews || 0,
      store_rating: product.store_rating,
      indexing_date: Math.floor(Date.now() / 1000),
      _highlightResult: generateHighlightResult(product, algoliaParams.query)
    }
  });
  
  const totalPages = Math.ceil(searchResults.pagination.total / algoliaParams.hitsPerPage);
  
  // 🔍 DEBUGGING TEMPORAL - Log de conversión de facetas
  const convertedFacets = searchResults.facets ? convertFacetsToAlgoliaFormat(searchResults.facets, algoliaParams.facets) : {};
  console.log('🔍 DEBUG - Converted facets:', convertedFacets);
  
  return {
    hits,
    nbHits: searchResults.pagination.total,
    page: algoliaParams.page,
    nbPages: totalPages,
    hitsPerPage: algoliaParams.hitsPerPage,
    facets: convertedFacets,
    facets_stats: searchResults.facets ? calculateFacetStats(searchResults.facets) : {},
    exhaustiveFacetsCount: true,
    exhaustiveNbHits: true,
    exhaustiveTypo: true,
    exhaustive: {
      facetsCount: true,
      nbHits: true,
      typo: true
    },
    query: algoliaParams.query || '',
    params: algoliaParams.originalParams || '',
    index: 'sears',
    processingTimeMS: 5,
    processingTimingsMS: {
      total: 5
    },
    serverTimeMS: 10
  }
}

function convertFacetsToAlgoliaFormat(facets, requestedFacets) {
  console.log('🔄 convertFacetsToAlgoliaFormat called with:');
  console.log('  - facets:', facets);
  console.log('  - requestedFacets:', requestedFacets);
  
  const algoliaFacets = {}
  
  if (!facets || !requestedFacets) {
    console.log('❌ Early return - missing facets or requestedFacets');
    return algoliaFacets;
  }
  
  requestedFacets.forEach(facetName => {
    console.log(`🔍 Processing facet: ${facetName}`);
    
    switch (facetName) {
      case 'brand':
        // FIX: Use 'brands' (plural) from your FacetService
        if (facets.brands) {
          algoliaFacets.brand = {}
          facets.brands.forEach(item => {
            algoliaFacets.brand[item.value] = parseInt(item.count);
          });
        }
        break
        
      case 'fulfillment':
        // FIX: Use 'fulfillmentTypes' from your FacetService
        if (facets.fulfillmentTypes) {
          algoliaFacets.fulfillment = {}
          facets.fulfillmentTypes.forEach(item => {
            algoliaFacets.fulfillment[item.value] = parseInt(item.count);
          });
        }
        break
        
      case 'has_free_shipping':
        // FIX: Extract from shippingOptions
        if (facets.shippingOptions) {
          algoliaFacets.has_free_shipping = {}
          facets.shippingOptions.forEach(item => {
            if (item.value === 'Envío Gratis') {
              algoliaFacets.has_free_shipping['true'] = parseInt(item.count);
            } else {
              // Sum all non-free shipping options
              algoliaFacets.has_free_shipping['false'] = (algoliaFacets.has_free_shipping['false'] || 0) + parseInt(item.count);
            }
          });
        }
        break
        
      case 'hirerarchical_category.lvl0':
        // FIX: Extract from categories
        if (facets.categories) {
          algoliaFacets['hirerarchical_category.lvl0'] = {}
          facets.categories.forEach(item => {
            algoliaFacets['hirerarchical_category.lvl0'][item.value] = parseInt(item.count);
          });
        }
        break
        
      case 'hirerarchical_category.lvl1':
        // This might need to be generated or come from a different source
        algoliaFacets['hirerarchical_category.lvl1'] = {}
        break
        
      case 'hirerarchical_category.lvl2':
        // This might need to be generated or come from a different source
        algoliaFacets['hirerarchical_category.lvl2'] = {}
        break
        
      case 'review_rating':
        // FIX: Use 'ratings' from your FacetService
        if (facets.ratings) {
          algoliaFacets.review_rating = {}
          facets.ratings.forEach(item => {
            algoliaFacets.review_rating[item.value] = parseInt(item.count);
          });
        }
        break
        
      case 'sale_price':
        // FIX: Use 'priceRanges' from your FacetService
        if (facets.priceRanges) {
          algoliaFacets.sale_price = {}
          facets.priceRanges.forEach(item => {
            // Use the min value as key
            algoliaFacets.sale_price[`${item.min}.0`] = parseInt(item.count);
          });
        }
        break
        
      case 'percent_off':
        // This would need to be added to your FacetService if you want discount ranges
        algoliaFacets.percent_off = {}
        break
        
      default:
        console.log(`  - Unhandled facet type: ${facetName}`);
    }
  });
  
  console.log('✅ Final algoliaFacets:', algoliaFacets);
  return algoliaFacets
}

/**
 * Calcular estadísticas de facetas
 */
function calculateFacetStats(facets) {
  const stats = {}
  
  Object.keys(facets).forEach(facetName => {
    const facetData = facets[facetName]
    if (typeof facetData === 'object') {
      const values = Object.values(facetData).filter(val => typeof val === 'number')
      if (values.length > 0) {
        stats[facetName] = {
          min: Math.min(...values),
          max: Math.max(...values),
          avg: values.reduce((sum, val) => sum + val, 0) / values.length,
          sum: values.reduce((sum, val) => sum + val, 0)
        }
      }
    }
  })
  
  return stats
}

/**
 * Generar highlight result para compatibilidad
 */
function generateHighlightResult(product, query) {
  return {
    title: {
      value: product.name,
      matchLevel: "none",
      matchedWords: []
    },
    brand: {
      value: product.brand,
      matchLevel: "none", 
      matchedWords: []
    },
    sku: {
      value: product.sku,
      matchLevel: "none",
      matchedWords: []
    }
  }
}

export { router as algoliaRoutes }
