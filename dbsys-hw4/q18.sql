set termout on;
set autotrace traceonly;
set timing on;

SELECT  
 c_name,
 c_custkey,
 o_orderkey,
 o_orderdate,
 o_totalprice,
 sum(l_quantity)
FROM
 customer,
 orders,
 lineitem
WHERE
  o_orderkey in (
   SELECT
           l_orderkey
   FROM 
           lineitem
   GROUP BY 
           l_orderkey having sum(l_quantity) > 300
  )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
GROUP BY
 c_name,
 c_custkey,
 o_orderkey,
 o_orderdate,
 o_totalprice;

quit;
