-- For each of the top 5 nations with the greatest value (i.e., sum of l_extendedprice) of orders placed, find ALL the nations that supply these orders.
-- Output schema: (name of nation acting as customer, name of nation acting as supplier, value of orders between supplier and customer)
-- Order by: (name of nation acting as customer, name of nation acting as supplier).
 
-- There are 2 changes to note:
-- 1) We ask you to sum over the l_extendedprice in lineitem, instead of using o_totalprice from orders.
-- 2) We ask for ALL nations in the second part of the description.
 
-- Because there are 25 nations in the database, the final result will now have 125 (5 x 25) rows, instead of 25 (5 x 5).


WITH topFiveBuyer AS
(select cn.n_name as custName, sn.n_name as suppName, sum(o_totalprice) as supp_totalprice
 from orders, lineitem, supplier, customer, partsupp, nation as cn, nation as sn,
    (select c_nationkey as t_nationkey, n_name as t_name, sum(l_extendedprice) as t_value
       from orders, customer, nation, lineitem
      where o_custkey = c_custkey and c_nationkey = n_nationkey and o_orderkey = l_orderkey
   group by c_nationkey, n_name
   order by t_value desc limit 5) as topCust
 where o_orderkey = l_orderkey and o_custkey = c_custkey and t_nationkey = c_nationkey
   and l_partkey = ps_partkey and l_suppkey = ps_suppkey and ps_suppkey = s_suppkey
   and cn.n_nationkey = c_nationkey and sn.n_nationkey = s_nationkey
 group by c_nationkey, s_nationkey
 order by supp_totalprice desc
)

select custName, suppName, supp_totalprice from topFiveBuyer
order by custName, suppName
;

