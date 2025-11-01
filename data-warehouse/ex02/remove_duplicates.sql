WITH duplicates AS (
    SELECT event_time,
           event_type,
           product_id,
           price,
           user_id,
           user_session,
           ROW_NUMBER() OVER (
               PARTITION BY event_time, event_type, product_id, price, user_id, user_session
               ORDER BY event_time
           ) as row_num
    FROM customers
)
DELETE FROM customers
WHERE (event_time, event_type, product_id, price, user_id, user_session) IN (
    SELECT event_time, event_type, product_id, price, user_id, user_session
    FROM duplicates
    WHERE row_num > 1
);