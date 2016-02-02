-- Find the top 10 parts that with the highest quantity in returned orders. 
-- An order is returned if the returnflag field on any lineitem part is the character R.
-- Output schema: (part key, part name, quantity returned)
-- Order by: by quantity returned, descending.

-- Student SQL code here:

select p_partkey, p_name, sum(l_quantity) as quantity
from part, lineitem  
where p_partkey = l_partkey and l_returnflag = 'R'
group by p_partkey, p_name
order by quantity desc 
limit 10;
