-- For each of the top 5 nations with the greatest value (i.e., total price) of orders placed,
-- find the top 5 nations which supply these orders.
-- Output schema: (Order placer name, Order supplier name, value of orders placed)
-- Order by: (Order placer, Order supplier)

-- Notes
--  1) We are expecting exactly 25 results 

-- Student SQL code here:


-- top 5 nations with the greatest value
-- (select c_nationkey as t_nationkey, n_name as t_name, sum(o_totalprice) as t_totalprice
-- from orders, customer, nation
-- where o_custkey = c_custkey and c_nationkey = n_nationkey
-- group by c_nationkey, n_name
-- order by t_totalprice desc limit 5)
WITH topFiveBuyer AS
(select cn.n_name as custName, sn.n_name as suppName, sum(o_totalprice) as supp_totalprice
 from orders, lineitem, supplier, customer, partsupp, nation as cn, nation as sn,
    (select c_nationkey as t_nationkey, n_name as t_name, sum(o_totalprice) as t_totalprice
       from orders, customer, nation
      where o_custkey = c_custkey and c_nationkey = n_nationkey
   group by c_nationkey, n_name
   order by t_totalprice desc limit 5) as topCust
 where o_orderkey = l_orderkey and o_custkey = c_custkey and t_nationkey = c_nationkey
   and l_partkey = ps_partkey and l_suppkey = ps_suppkey and ps_suppkey = s_suppkey
   and cn.n_nationkey = c_nationkey and sn.n_nationkey = s_nationkey
 group by c_nationkey, s_nationkey
)

select custName, suppName, supp_totalprice
 from topFiveBuyer
where (
    select count(*) from topFiveBuyer as t
    where t.custName = topFiveBuyer.custName and t.supp_totalprice <= topFiveBuyer.supp_totalprice
) <= 5
order by custName, suppName
 ;
 