-- Find the name of the most heavily ordered (i.e., highest quantity) part per nation.
-- Output schema: (nation key, nation name, part key, part name, quantity ordered)
-- Order by: (nation key, part key) SC

-- Notes
--   1) You may use a SQL 'WITH' clause for common table expressions.
--   2) A single nation may have more than 1 most-heavily-ordered-part.

-- Student SQL code here:

-- select n_nationkey, n_name, p_partkey, p_name, sum(l_quantity) as quantity
-- from part, orders, lineitem, customer, nation
-- where n_nationkey = c_nationkey and c_custkey = o_custkey and o_orderkey = l_orderkey and l_partkey = p_partkey
-- group by n_nationkey, n_name, p_partkey, p_name
-- order by quantity desc;

WITH nationPart AS 
(select n_nationkey as np_nationkey, p_partkey as np_partkey, sum(l_quantity) as np_quantity
 from nation, orders, lineitem, customer, part
 where n_nationkey = c_nationkey and c_custkey = o_custkey and o_orderkey = l_orderkey and l_partkey = p_partkey
 group by np_nationkey, np_partkey
 order by np_quantity desc)

select np_nationkey, n_name, np_partkey, p_name, np_quantity 
from nationPart, nation, part
where np_nationkey = n_nationkey and np_partkey = p_partkey
  and np_quantity >= (select max(np_quantity) from nationPart where np_nationkey = n_nationkey)
group by np_quantity, n_name, np_partkey, p_name
order by np_nationkey, np_partkey desc limit 1;


WITH nationPart AS 
(select n_nationkey as np_nationkey, n_name as np_name, p_partkey as np_partkey, p_partname as np_partname, sum(l_quantity) as np_quantity
 from nation, orders, lineitem, customer, part
 where n_nationkey = c_nationkey and c_custkey = o_custkey and o_orderkey = l_orderkey and l_partkey = p_partkey
 group by np_nationkey, np_name, np_partkey, np_partname)

select np_nationkey, n_name, np_partkey, p_name, np_quantity 
from nationPart, nation, part
where np_nationkey = n_nationkey and np_partkey = p_partkey
  and np_quantity >= (select max(np_quantity) from nationPart where np_nationkey = n_nationkey)
group by np_quantity, n_name, np_partkey, p_name
order by np_nationkey, np_partkey desc limit 1;