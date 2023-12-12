SELECT DISTINCT occupation
FROM customers
WHERE occupation LIKE '%Engineer%'
ORDER BY occupation ASC;