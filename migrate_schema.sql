-- Migration script to update existing PostgreSQL schema from product (TEXT) to product_id (INTEGER FK)
-- Run this only if you have an existing deployment with the old schema

BEGIN;

-- Step 1: Add product_id column temporarily allowing NULLs
ALTER TABLE orders ADD COLUMN product_id_tmp INTEGER;

-- Step 2: Create a temporary mapping from product names to product IDs
-- This assumes products.product_name matches the old orders.product values
UPDATE orders o
SET product_id_tmp = p.id
FROM products p
WHERE o.product = p.product_name;

-- Step 3: Check for any orders that couldn't be mapped (optional - will fail if there are unmapped orders)
DO $$
DECLARE
    unmapped_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO unmapped_count FROM orders WHERE product_id_tmp IS NULL;
    IF unmapped_count > 0 THEN
        RAISE EXCEPTION 'Found % orders with unmapped product names. Please fix data before continuing.', unmapped_count;
    END IF;
END $$;

-- Step 4: Drop the old product column
ALTER TABLE orders DROP COLUMN product;

-- Step 5: Rename product_id_tmp to product_id
ALTER TABLE orders RENAME COLUMN product_id_tmp TO product_id;

-- Step 6: Add NOT NULL constraint
ALTER TABLE orders ALTER COLUMN product_id SET NOT NULL;

-- Step 7: Add foreign key constraint
ALTER TABLE orders 
ADD CONSTRAINT orders_product_id_fkey 
FOREIGN KEY (product_id) 
REFERENCES products(id) 
ON DELETE RESTRICT;

-- Step 8: Create index on product_id for better join performance
CREATE INDEX IF NOT EXISTS idx_orders_product_id ON orders(product_id);

COMMIT;

-- Verify the migration
SELECT 
    tc.table_name, 
    kcu.column_name, 
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name 
FROM 
    information_schema.table_constraints AS tc 
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY' 
AND tc.table_name = 'orders';
