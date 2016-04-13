


import Database, shutil, Storage
db = Database.Database()

"""
TPC-H Query 6: a 4-chain filter and aggregate query
select
        sum(l_extendedprice * l_discount) as revenue
from
        lineitem
where
        l_shipdate >= 19940101
        and l_shipdate < 19950101
        and l_discount between 0.06 - 0.01 and 0.06 + 0.01
        and l_quantity < 24

"""

lineitemSchema = db.relationSchema('lineitem')


