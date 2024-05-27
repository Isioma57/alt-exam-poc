--                                             PART 2a


--i)
-- This query finds the most ordered item based on the number of times it appears in an order cart that checked out successfully

-- Define a CTE to extract relevant data from the "events" table,
-- converting JSON fields to appropriate data types for joining and filtering.
WITH event_activities AS (
    SELECT
        (event_data->> 'item_id')::bigint AS item_id,    -- Convert item_id to bigint for compatibility with item_id column in line_items table
        (event_data->> 'order_id')::uuid AS order_id,    -- Convert order_id to uuid for compatibility with order_id column in line_items table
        event_data->> 'event_type' AS event_type,        -- Extract event type to filter for checkout events
        event_data->> 'status' AS status                 -- Extract status to filter for successful transactions
    FROM 
        alt_school.events
),

-- Define a second CTE to join line items with products, collecting the needed product details
product_items AS (
    SELECT
        l.order_id,                                      -- Select order_id for joining with event data
        p.id AS product_id,                              -- Include product_id for final output and grouping
        p.name AS product_name                           -- Include product name for final output
    FROM 
        alt_school.line_items AS l
    JOIN 
        alt_school.products AS p ON l.item_id = p.id               -- Join products on item_id to fetch product details
)

-- Main query to calculate the number of times each product appears in successful orders
SELECT 
    product_items.product_id AS product_id,            -- Display product_id in the output
    product_items.product_name AS product_name,        -- Display product_name in the output
    COUNT(product_items.product_id) AS num_times_in_successful_orders -- Count occurrences of each product in successful orders
FROM 
    event_activities
JOIN 
    product_items ON event_activities.order_id = product_items.order_id -- Join on order_id to align events with their line items
WHERE 
    event_activities.event_type ='checkout'                 -- Filter for checkout events
    AND event_activities.status = 'success'                 -- Filter for events that were successful
GROUP BY 
    product_items.product_id, 
    product_items.product_name 						   -- Group results by product_id and product_name to aggregate counts
ORDER BY 
    num_times_in_successful_orders DESC                -- Order results by the count of successful orders, highest first
LIMIT 1;                                              -- Limit the result to the most frequently ordered product







--ii)
-- This query finds the top 5 spenders

-- CTE to gather event data, focusing on events where items were added to cart and not removed
WITH customer_event AS (
    SELECT
        customer_id,                                                       -- Select customer ID for further joins and aggregation
        (event_data->>'item_id')::bigint AS item_id,                       -- Extract and convert item_id from JSON for joining with products
        p.name AS product_name,                                            -- Get the product name directly from the products table
        (event_data->>'quantity')::bigint AS quantity_of_items,            -- Extract and convert the quantity involved in the event
        SUM(CASE WHEN event_data->>'event_type' = 'add_to_cart' THEN 1 ELSE 0 END) AS total_added_quantity, -- Count of 'add_to_cart' actions
        p.price                                                            -- Product price for calculating total spend
    FROM
        alt_school.events AS e
    JOIN
        alt_school.products AS p ON (e.event_data->>'item_id')::bigint = p.id         -- Join products on item_id to get product details
    WHERE
        event_data->>'event_type' IN ('add_to_cart', 'remove_from_cart')   -- Filter for event types that are add to cart and remove from cart
    GROUP BY
        customer_id,
        item_id,
        p.name,
        quantity_of_items,
        p.price
    HAVING
        SUM(CASE WHEN event_data->>'event_type' = 'remove_from_cart' THEN 1 ELSE 0 END) = 0      -- Filter out any items that were removed
),

-- CTE to gather customer location and successful order data
customer_location AS (
    SELECT
        c.customer_id,
        o.order_id,
        c.location,                                        -- Customer's location for final output
        o.status                                           -- Order status to filter for successful orders
    FROM
        alt_school.customers AS c
    JOIN
        alt_school.orders AS o ON c.customer_id = o.customer_id      -- Join orders to get order details related to customers
    WHERE
        o.status = 'success'                              -- Ensure we only consider successful orders
)

-- Main query to calculate the total spend per customer and rank the top spenders
SELECT
    customer_location.customer_id AS customer_id,
    customer_location.location AS location,                -- Include customer location in the output
    SUM(customer_event.quantity_of_items * customer_event.price) AS total_spend -- Calculate total spend
FROM
    customer_event
JOIN
    customer_location ON customer_event.customer_id = customer_location.customer_id -- Join on customer_id to correlate events with customer location
GROUP BY
    customer_location.customer_id,
    customer_location.location                              -- Group by customer and location for aggregation
ORDER BY
    total_spend DESC                                        -- Order by total spend to identify top spenders
LIMIT 5;                                                    -- Limit to top 5 spenders







--                                            PART 2b

--i)
--This query determines the most common location (country) where successful checkouts occurred

-- Selecting the location and count of successful checkouts from events joined with customer data
SELECT 
	c.location AS location,                                    -- Output the customer location as 'location'
	COUNT(event_data->> 'status') AS checkout_count            -- Count the number of events where 'status' is 'success'
FROM 
	alt_school.events AS e                                     -- From the events table, aliased as 'e'
JOIN 
	alt_school.customers AS c                                  -- Join the customers table, aliased as 'c'
ON 
	e.customer_id = c.customer_id                              -- Join condition on customer_id to match events with customers
WHERE 
	event_data->> 'status' = 'success'                         -- Filter to include only events where the status is 'success'
GROUP BY 
	location                                                   -- Group the results by customer location
ORDER BY 
	checkout_count DESC                                        -- Order the results by the checkout count in descending order
LIMIT 1;                                                           -- Limit the results to only the top location







--ii)
-- This query calculates the number of 'add to cart' or 'remove from cart' events for each customer
-- who has not completed a checkout, and then orders the results to find the customers with the most of such events.
SELECT 
    e.customer_id AS customer_id,                                          -- Selecting the customer ID from the events table
    COUNT(*) AS num_events                                  -- Counting the total number of qualifying events for each customer
FROM 
    alt_school.events e                                     -- From the events table, aliased as 'e' for ease of reference
LEFT JOIN (
    SELECT DISTINCT customer_id                             -- Selecting distinct customer IDs who have completed a checkout
    FROM alt_school.events
    WHERE event_data->>'event_type' = 'checkout'            -- Filtering events to only include those where the event type is 'checkout'
) as checkout_events ON e.customer_id = checkout_events.customer_id -- Left joining to identify customers without a checkout event
WHERE 
    e.event_data->>'event_type' IN ('add_to_cart', 'remove_from_cart')      -- Filtering for events that are either cart additions or removals
    AND checkout_events.customer_id IS NULL                 -- Ensuring that only customers who have NOT checked out are included
GROUP BY 
    e.customer_id                                           -- Grouping the results by customer ID to aggregate events per customer
ORDER BY 
    num_events DESC;                                         -- Ordering the results by the count of events in descending order to identify top results







--iii)
-- This query calculates the average number of visits per customer before making a successful transaction

-- Define a CTE to identify customers who have successfully completed a checkout
WITH checkout_customers AS (
    SELECT DISTINCT customer_id                         -- Select unique customer IDs
    FROM alt_school.events
    WHERE 
        event_data->>'event_type' = 'checkout' AND      -- Filter events to only those marked as 'checkout'
        event_data->> 'status' = 'success'              -- Ensure the checkout was successful
),

-- Define another CTE to calculate the number of visits for each customer 
customer_visits AS (
    SELECT 
        customer_id, 
        COUNT(*) AS num_visits                          -- Count the total number of visit events for each customer
    FROM 
        alt_school.events
    WHERE 
        customer_id IN (SELECT customer_id FROM checkout_customers) -- Filter for customers who are in the checkout_customers CTE
        AND event_data->>'event_type' = 'visit'         -- Ensure the event type is a 'visit'
    GROUP BY 
        customer_id                                     -- Group results by customer_id to aggregate visits
)

-- Main query to calculate the average number of visits of customers who has completed a successful checkout
SELECT ROUND(AVG(num_visits)::numeric, 2) AS average_visits  -- Calculate the average and round to two decimal places
FROM customer_visits;                                        -- From the customer_visits CTE
