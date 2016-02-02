-- Find the name of the most heavily ordered (i.e., highest quantity) part per nation.
-- Output schema: (nation key, nation name, part key, part name, quantity ordered)
-- Order by: (nation key, part key) SC

-- Notes
--   1) You may use a SQL 'WITH' clause for common table expressions.
--   2) A single nation may have more than 1 most-heavily-ordered-part.

-- Student SQL code here:

WITH nationPart AS 
(select n_nationkey as nationkey, n_name as name, p_partkey as partkey, p_name as partname, sum(l_quantity) as quantity
 from nation, orders, lineitem, customer, part
 where n_nationkey = c_nationkey and c_custkey = o_custkey and o_orderkey = l_orderkey and l_partkey = p_partkey
 group by nationkey, name, partkey, partname)

select n.nationkey, n.name, n.partkey, n.partname, n.quantity
from nationPart as n,
    (select nationkey, max(quantity) as maxQuantity from nationPart group by nationkey) res
where n.nationkey = res.nationkey and n.quantity >= res.maxQuantity
order by n.nationkey, n.partkey;





