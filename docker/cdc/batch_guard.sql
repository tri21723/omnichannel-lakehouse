-- Batch guard (Phase 8): row count + md5 of every Walmart table per batch dataset prefix.
-- CDC scenarios may only touch their own prefix; this output must not change. Read-only.
WITH runs(run_id) AS (
    VALUES ('186ae107d0793d2ee237'), ('b1e64afaff0176de180a'), ('5f552019ee41c24af95f')
),
orders AS (
    SELECT r.run_id, count(o.*) AS n,
           md5(coalesce(string_agg(row(o.*)::text, '|' ORDER BY o.order_id), '')) AS h
    FROM runs r LEFT JOIN walmart.orders o ON starts_with(o.order_id, r.run_id || '-walmart-')
    GROUP BY r.run_id
),
items AS (
    SELECT r.run_id, count(i.*) AS n,
           md5(coalesce(string_agg(row(i.*)::text, '|' ORDER BY i.order_id, i.line_no), '')) AS h
    FROM runs r LEFT JOIN walmart.order_items i ON starts_with(i.order_id, r.run_id || '-walmart-')
    GROUP BY r.run_id
),
rets AS (
    SELECT r.run_id, count(x.*) AS n,
           md5(coalesce(string_agg(row(x.*)::text, '|' ORDER BY x.return_id, x.return_line_no), '')) AS h
    FROM runs r LEFT JOIN walmart.returns x ON starts_with(x.order_id, r.run_id || '-walmart-')
    GROUP BY r.run_id
),
custs AS (
    SELECT r.run_id, count(c.*) AS n,
           md5(coalesce(string_agg(row(c.*)::text, '|' ORDER BY c.customer_id), '')) AS h
    FROM runs r LEFT JOIN walmart.customers c ON starts_with(c.customer_id, r.run_id || '-walmart-')
    GROUP BY r.run_id
),
prods AS (
    SELECT r.run_id, count(p.*) AS n,
           md5(coalesce(string_agg(row(p.*)::text, '|' ORDER BY p.product_id), '')) AS h
    FROM runs r LEFT JOIN walmart.products p ON starts_with(p.product_id, r.run_id || '-walmart-')
    GROUP BY r.run_id
)
SELECT o.run_id,
       o.n AS orders, o.h AS orders_md5,
       i.n AS order_items, i.h AS order_items_md5,
       x.n AS returns, x.h AS returns_md5,
       c.n AS customers, c.h AS customers_md5,
       p.n AS products, p.h AS products_md5
FROM orders o
JOIN items i USING (run_id)
JOIN rets x USING (run_id)
JOIN custs c USING (run_id)
JOIN prods p USING (run_id)
ORDER BY o.run_id;
