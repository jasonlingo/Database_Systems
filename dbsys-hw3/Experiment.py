import Database
from Catalog.Schema import DBSchema
import time

def getResult(db, query):
    return [query.schema().unpack(tup) for page in db.processQuery(query) for tup in page[1]]


if __name__=="__main__":

  db = Database.Database(dataDir='./data')

  """
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


  # tables
  customer = db.query().fromTable('customer')
  nation   = db.query().fromTable('nation')
  orders   = db.query().fromTable('orders')
  lineitem = db.query().fromTable('lineitem')
  part     = db.query().fromTable('part')

  groupKeySchema = DBSchema('groupKey', [('ONE', 'int')]);
  groupAggSchema = DBSchema('groupBy', [('revenue','float')]);
  query1 = db.query().fromTable('lineitem').where( \
              "(L_SHIPDATE >= 19940101) and (L_SHIPDATE < 19950101) and \
              (0.06 - 0.01 <= L_DISCOUNT <= 0.06 + 0.01) and (L_QUANTITY < 24)").groupBy( \
              groupSchema=groupKeySchema, \
              aggSchema=groupAggSchema, \
              groupExpr=(lambda e: 1), \
              aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * e.L_DISCOUNT), lambda x: x)], \
              groupHashFn=(lambda gbVal: hash(gbVal) % 1)).select( \
              {'revenue' : ('revenue', 'float')}).finalize();
