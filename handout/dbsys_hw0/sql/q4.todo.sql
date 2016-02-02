-- For each of the top 5 nations with the greatest value (i.e., sum of l_extendedprice) of orders placed, find ALL the nations that supply these orders.
-- Output schema: (name of nation acting as customer, name of nation acting as supplier, value of orders between supplier and customer)
-- Order by: (name of nation acting as customer, name of nation acting as supplier).
 
-- There are 2 changes to note:
-- 1) We ask you to sum over the l_extendedprice in lineitem, instead of using o_totalprice from orders.
-- 2) We ask for ALL nations in the second part of the description.
 
-- Because there are 25 nations in the database, the final result will now have 125 (5 x 25) rows, instead of 25 (5 x 5).