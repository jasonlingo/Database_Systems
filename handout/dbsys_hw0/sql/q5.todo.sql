-- Find the customer market segments where the yearly total number of orders declines 
-- in the last 2 years in the dataset. Note the database will have different date 
-- ranges per market segment, for example segment A records between 1990-1995, and 
-- segment B between 1992-1998. That is for segment A, we want the difference between 
-- 1995 and 1994.
-- Output schema: (market segment, last year for segment, difference in # orders)
-- Order by: market segment ASC 

-- Notes
--  1) Use the sqlite function strftime('%Y', <text>) to extract the year from a text field representing a date.
--  2) Use CAST(<text> as INTEGER) to convert text (e.g., a year) into an INTEGER.
--  3) You may use a SQL 'WITH' clause.

-- Student SQL code here:

WITH lastYear AS
(select c_mktsegment as l_mktsegment, max(year) as l_year, totalOrder as l_totalOrder
 from 
    (
     select c_mktsegment, cast(strftime('%Y', o_orderdate) as INTEGER) as year, count(*) as totalOrder
       from customer join orders on c_custkey = o_custkey
     group by c_mktsegment, year
    )
  group by l_mktsegment
)

select l_mktsegment, l_year, l2_year, (l_totalOrder - l2_totalOrder) as difference
from lastYear,
   ( -- one year before last year
     select c_mktsegment as l2_mktsegment, cast(strftime('%Y', o_orderdate) as INTEGER) as l2_year, count(*) as l2_totalOrder
       from customer, orders, lastYear
      where c_custkey = o_custkey and l_mktsegment = c_mktsegment and l_year - l2_year = 1
     group by l2_mktsegment, l2_year
   ) as last2Year
where l_mktsegment = l2_mktsegment and l_totalOrder - l2_totalOrder < 0
order by l_mktsegment asc
;





