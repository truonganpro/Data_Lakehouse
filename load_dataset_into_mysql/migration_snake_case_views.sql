-- Migration Views for Snake Case Tables
-- Create views with old camelCase names for backward compatibility
-- These views can be dropped after 1-2 weeks once all queries are migrated

-- Note: These views are temporary and should be removed after migration period

-- 1. dm_sales_monthly_category (old: dmsalesmonthlycategory)
CREATE OR REPLACE VIEW lakehouse.platinum.dmsalesmonthlycategory AS 
    SELECT * FROM lakehouse.platinum.dm_sales_monthly_category;

-- 2. dm_seller_kpi (old: dmsellerkpi)
CREATE OR REPLACE VIEW lakehouse.platinum.dmsellerkpi AS 
    SELECT * FROM lakehouse.platinum.dm_seller_kpi;

-- 3. dm_customer_lifecycle (old: dmcustomerlifecycle)
CREATE OR REPLACE VIEW lakehouse.platinum.dmcustomerlifecycle AS 
    SELECT * FROM lakehouse.platinum.dm_customer_lifecycle;

-- 4. dm_payment_mix (old: dmpaymentmix)
CREATE OR REPLACE VIEW lakehouse.platinum.dmpaymentmix AS 
    SELECT * FROM lakehouse.platinum.dm_payment_mix;

-- 5. dm_logistics_sla (old: dmlogisticssla)
CREATE OR REPLACE VIEW lakehouse.platinum.dmlogisticssla AS 
    SELECT * FROM lakehouse.platinum.dm_logistics_sla;

-- 6. dm_product_bestsellers (old: dmproductbestsellers)
CREATE OR REPLACE VIEW lakehouse.platinum.dmproductbestsellers AS 
    SELECT * FROM lakehouse.platinum.dm_product_bestsellers;

-- 7. dm_category_price_bands (old: dmcategorypricebands)
CREATE OR REPLACE VIEW lakehouse.platinum.dmcategorypricebands AS 
    SELECT * FROM lakehouse.platinum.dm_category_price_bands;

-- Verification: Check if views are created
-- SELECT table_name, table_type 
-- FROM information_schema.tables 
-- WHERE table_schema = 'platinum' 
--   AND table_name LIKE 'dm%'
-- ORDER BY table_name;

-- To drop views after migration period:
-- DROP VIEW IF EXISTS lakehouse.platinum.dmsalesmonthlycategory;
-- DROP VIEW IF EXISTS lakehouse.platinum.dmsellerkpi;
-- DROP VIEW IF EXISTS lakehouse.platinum.dmcustomerlifecycle;
-- DROP VIEW IF EXISTS lakehouse.platinum.dmpaymentmix;
-- DROP VIEW IF EXISTS lakehouse.platinum.dmlogisticssla;
-- DROP VIEW IF EXISTS lakehouse.platinum.dmproductbestsellers;
-- DROP VIEW IF EXISTS lakehouse.platinum.dmcategorypricebands;

