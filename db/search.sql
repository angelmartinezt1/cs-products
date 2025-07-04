-- BASE DE DATOS OPTIMIZADA PARA BÚSQUEDAS (Reemplazo de Algolia)
-- Esta BD está diseñada para ser súper rápida en consultas de búsqueda

-- 1. TABLA PRINCIPAL DE PRODUCTOS (desnormalizada para velocidad)
CREATE TABLE products (
    id INT PRIMARY KEY,
    
    -- Información básica
    name VARCHAR(500) NOT NULL,
    description TEXT,
    short_description VARCHAR(1000),
    sku VARCHAR(100),
    brand VARCHAR(100),
    
    -- Precios
    sales_price DECIMAL(10,2),
    list_price DECIMAL(10,2),
    shipping_cost DECIMAL(10,2) DEFAULT 0,
    percentage_discount TINYINT DEFAULT 0,
    
    -- Stock y disponibilidad
    stock INT DEFAULT 0,
    status TINYINT DEFAULT 1,
    visible TINYINT DEFAULT 1,
    
    -- Categorías (desnormalizadas para velocidad)
    category_id INT,
    category_name VARCHAR(255),
    category_lvl0 VARCHAR(255),
    category_lvl1 VARCHAR(500),
    category_lvl2 VARCHAR(750),
    category_path TEXT, -- Ruta completa: "electrónica > tv > televisiones"
    
    -- Tienda/Seller
    store_id INT,
    store_name VARCHAR(255),
    store_logo VARCHAR(500),
    store_rating DECIMAL(3,2),
    store_authorized TINYINT DEFAULT 1,
    
    -- Características del producto
    digital TINYINT DEFAULT 0,
    big_ticket TINYINT DEFAULT 0,
    back_order TINYINT DEFAULT 0,
    is_store_pickup TINYINT DEFAULT 0,
    super_express TINYINT DEFAULT 0,
    is_store_only TINYINT DEFAULT 0,
    shipping_days TINYINT DEFAULT 5,
    
    -- Reviews y ratings
    review_rating DECIMAL(3,2),
    total_reviews INT DEFAULT 0,
    
    -- Imágenes
    main_image VARCHAR(500),
    thumbnail VARCHAR(500),
    
    -- Fulfillment
    fulfillment_type ENUM('seller', 'fulfillment') DEFAULT 'seller',
    
    -- Campos calculados para facetas
    has_free_shipping TINYINT GENERATED ALWAYS AS (shipping_cost = 0) STORED,
    price_range VARCHAR(20) GENERATED ALWAYS AS (
        CONCAT(FLOOR(sales_price/1000)*1000, '-', FLOOR(sales_price/1000)*1000+999)
    ) STORED,
    discount_range VARCHAR(10) GENERATED ALWAYS AS (
        CASE 
            WHEN percentage_discount = 0 THEN NULL
            ELSE CONCAT(FLOOR(percentage_discount/10)*10, '-', FLOOR(percentage_discount/10)*10+9)
        END
    ) STORED,
    rating_range VARCHAR(10) GENERATED ALWAYS AS (
        CASE 
            WHEN review_rating IS NULL THEN NULL
            ELSE CONCAT(FLOOR(review_rating), '-', CEIL(review_rating))
        END
    ) STORED,
    
    -- Texto para búsqueda (concatenado)
    search_text TEXT GENERATED ALWAYS AS (
        CONCAT_WS(' ', name, description, brand, sku, category_name)
    ) STORED,
    
    -- Metadatos
    relevance_score DECIMAL(5,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Índices optimizados
    INDEX idx_status_visible (status, visible),
    INDEX idx_store (store_id, store_authorized),
    INDEX idx_category (category_id),
    INDEX idx_brand (brand),
    INDEX idx_price (sales_price),
    INDEX idx_stock (stock),
    INDEX idx_shipping (has_free_shipping),
    INDEX idx_fulfillment (fulfillment_type),
    INDEX idx_rating (review_rating),
    INDEX idx_relevance (relevance_score),
    INDEX idx_category_lvl0 (category_lvl0),
    INDEX idx_category_lvl1 (category_lvl1),
    INDEX idx_category_lvl2 (category_lvl2),
    INDEX idx_price_range (price_range),
    INDEX idx_discount_range (discount_range),
    INDEX idx_rating_range (rating_range),
    
    -- Índice compuesto para consultas principales
    INDEX idx_main_search (status, visible, store_authorized, relevance_score),
    INDEX idx_category_search (category_id, status, visible, relevance_score),
    INDEX idx_brand_search (brand, status, visible, relevance_score),
    
    -- Índice FULLTEXT para búsqueda de texto
    FULLTEXT INDEX idx_fulltext_search (search_text),
    FULLTEXT INDEX idx_fulltext_name (name),
    FULLTEXT INDEX idx_fulltext_name_brand (name, brand)
);

-- 2. TABLA DE FACETAS PRE-CALCULADAS (para respuesta ultra-rápida)
-- SOLUCIÓN: Usar un campo calculado en lugar de función en PRIMARY KEY
CREATE TABLE facet_counts (
    facet_type VARCHAR(50),
    facet_value VARCHAR(255),
    facet_count INT,
    category_id INT DEFAULT NULL,
    -- Campo calculado para manejar NULLs en la clave primaria
    category_key INT GENERATED ALWAYS AS (COALESCE(category_id, 0)) STORED,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    PRIMARY KEY (facet_type, facet_value, category_key),
    INDEX idx_facet_type (facet_type),
    INDEX idx_category (category_id),
    INDEX idx_count (facet_count DESC)
);

-- 3. TABLA DE VARIACIONES/TALLAS (simplificada)
CREATE TABLE product_variations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT,
    sku VARCHAR(100),
    size_name VARCHAR(50),
    color_name VARCHAR(50),
    stock INT DEFAULT 0,
    price_modifier DECIMAL(10,2) DEFAULT 0,
    
    INDEX idx_product (product_id),
    INDEX idx_sku (sku),
    INDEX idx_stock (stock),
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

-- 4. TABLA DE IMÁGENES ADICIONALES
CREATE TABLE product_images (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT,
    image_url VARCHAR(500),
    thumbnail_url VARCHAR(500),
    image_order TINYINT DEFAULT 1,
    
    INDEX idx_product (product_id),
    INDEX idx_order (image_order),
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

-- 5. TABLA DE ATRIBUTOS/ESPECIFICACIONES
CREATE TABLE product_attributes (
    product_id INT,
    attribute_name VARCHAR(100),
    attribute_value VARCHAR(500),
    
    PRIMARY KEY (product_id, attribute_name),
    INDEX idx_attribute (attribute_name),
    INDEX idx_value (attribute_value),
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

-- 6. VISTA OPTIMIZADA PARA BÚSQUEDAS
CREATE VIEW v_search_products AS
SELECT 
    p.*,
    GROUP_CONCAT(DISTINCT pi.image_url ORDER BY pi.image_order SEPARATOR '|') as additional_images,
    COUNT(DISTINCT pv.id) as variation_count,
    SUM(pv.stock) as total_variation_stock
FROM products p
LEFT JOIN product_images pi ON pi.product_id = p.id
LEFT JOIN product_variations pv ON pv.product_id = p.id
WHERE p.status = 1 AND p.visible = 1 AND p.store_authorized = 1
GROUP BY p.id;

-- PROCEDIMIENTOS PARA ACTUALIZAR FACETAS (ejecutar periódicamente)
DELIMITER //

-- Actualizar conteos de marcas
CREATE PROCEDURE UpdateBrandFacets()
BEGIN
    DELETE FROM facet_counts WHERE facet_type = 'brand';
    
    INSERT INTO facet_counts (facet_type, facet_value, facet_count, category_id)
    SELECT 'brand', brand, COUNT(*), NULL
    FROM products 
    WHERE status = 1 AND visible = 1 AND store_authorized = 1 AND brand IS NOT NULL
    GROUP BY brand;
END //

-- Actualizar conteos de categorías
CREATE PROCEDURE UpdateCategoryFacets()
BEGIN
    DELETE FROM facet_counts WHERE facet_type LIKE 'category_%';
    
    -- Nivel 0
    INSERT INTO facet_counts (facet_type, facet_value, facet_count, category_id)
    SELECT 'category_lvl0', category_lvl0, COUNT(*), NULL
    FROM products 
    WHERE status = 1 AND visible = 1 AND store_authorized = 1 AND category_lvl0 IS NOT NULL
    GROUP BY category_lvl0;
    
    -- Nivel 1
    INSERT INTO facet_counts (facet_type, facet_value, facet_count, category_id)
    SELECT 'category_lvl1', category_lvl1, COUNT(*), NULL
    FROM products 
    WHERE status = 1 AND visible = 1 AND store_authorized = 1 AND category_lvl1 IS NOT NULL
    GROUP BY category_lvl1;
    
    -- Nivel 2
    INSERT INTO facet_counts (facet_type, facet_value, facet_count, category_id)
    SELECT 'category_lvl2', category_lvl2, COUNT(*), NULL
    FROM products 
    WHERE status = 1 AND visible = 1 AND store_authorized = 1 AND category_lvl2 IS NOT NULL
    GROUP BY category_lvl2;
END //

-- Actualizar todas las facetas
CREATE PROCEDURE UpdateAllFacets()
BEGIN
    CALL UpdateBrandFacets();
    CALL UpdateCategoryFacets();
    
    -- Otras facetas
    DELETE FROM facet_counts WHERE facet_type IN ('fulfillment', 'shipping', 'price_range', 'discount_range', 'rating_range', 'store');
    
    -- Fulfillment
    INSERT INTO facet_counts (facet_type, facet_value, facet_count, category_id)
    SELECT 'fulfillment', fulfillment_type, COUNT(*), NULL
    FROM products 
    WHERE status = 1 AND visible = 1 AND store_authorized = 1
    GROUP BY fulfillment_type;
    
    -- Shipping
    INSERT INTO facet_counts (facet_type, facet_value, facet_count, category_id)
    SELECT 'shipping', IF(has_free_shipping = 1, 'free', 'paid'), COUNT(*), NULL
    FROM products 
    WHERE status = 1 AND visible = 1 AND store_authorized = 1
    GROUP BY has_free_shipping;
    
    -- Price ranges
    INSERT INTO facet_counts (facet_type, facet_value, facet_count, category_id)
    SELECT 'price_range', price_range, COUNT(*), NULL
    FROM products 
    WHERE status = 1 AND visible = 1 AND store_authorized = 1 AND price_range IS NOT NULL
    GROUP BY price_range;
    
    -- Discount ranges
    INSERT INTO facet_counts (facet_type, facet_value, facet_count, category_id)
    SELECT 'discount_range', discount_range, COUNT(*), NULL
    FROM products 
    WHERE status = 1 AND visible = 1 AND store_authorized = 1 AND discount_range IS NOT NULL
    GROUP BY discount_range;
    
    -- Rating ranges
    INSERT INTO facet_counts (facet_type, facet_value, facet_count, category_id)
    SELECT 'rating_range', rating_range, COUNT(*), NULL
    FROM products 
    WHERE status = 1 AND visible = 1 AND store_authorized = 1 AND rating_range IS NOT NULL
    GROUP BY rating_range;
    
    -- Stores
    INSERT INTO facet_counts (facet_type, facet_value, facet_count, category_id)
    SELECT 'store', CAST(store_id AS CHAR), COUNT(*), NULL
    FROM products 
    WHERE status = 1 AND visible = 1 AND store_authorized = 1
    GROUP BY store_id;
END //

DELIMITER ;