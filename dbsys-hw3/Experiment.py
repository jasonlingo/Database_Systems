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
          and l_discount betwgeen 0.06 - 0.01 and 0.06 + 0.01
          and l_quantity < 24

  """


  # tables
  # customer = db.query().fromTable('customer')
  # nation   = db.query().fromTable('nation')
  # orders   = db.query().fromTable('orders')
  # lineitem = db.query().fromTable('lineitem')
  # part     = db.query().fromTable('part')

  groupKeySchema = DBSchema('groupKey', [('ONE', 'int')])
  groupAggSchema = DBSchema('groupBy', [('revenue','float')])
  query1 = db.query().fromTable('lineitem').where(
              "(L_SHIPDATE >= 19940101) and (L_SHIPDATE < 19950101) and \
              (0.06 - 0.01 <= L_DISCOUNT <= 0.06 + 0.01) and (L_QUANTITY < 24)").groupBy(
              groupSchema=groupKeySchema,
              aggSchema=groupAggSchema,
              groupExpr=(lambda e: 1),
              aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * e.L_DISCOUNT), lambda x: x)],
              groupHashFn=(lambda gbVal: hash(gbVal) % 1)).select(
              {'revenue' : ('revenue', 'float')}).finalize()


  """
  select
          l_orderkey,
          sum(l_extendedprice * (1 - l_discount)) as revenue,
          o_orderdate,
          o_shippriority
  from
          customer,
          orders,
          lineitem
  where
          c_mktsegment = 'BUILDING'
          and c_custkey = o_custkey
          and l_orderkey = o_orderkey
          and o_orderdate < 19950315
          and l_shipdate > 19950315
  group by
          l_orderkey,
          o_orderdate,
          o_shippriority
  """
  ls1 = DBSchema('customerKey1', [('C_CUSTKEY', 'int')])
  rs1 = DBSchema('customerKey2', [('O_CUSTKEY', 'int')])

  ls2 = DBSchema('orderKey1', [('O_ORDERKEY', 'int')])
  rs2 = DBSchema('orderkey2', [('L_ORDERKEY', 'int')])

  groupKeySchema = DBSchema('groupKey', [('L_ORDERKEY', 'int'), ('O_ORDERDATE', 'int'), ('O_SHIPPRIORITY', 'int')])
  groupAggSchema = DBSchema('groupAgg', [('revenue','float')])

  print ("Processing query3...")
  query3 = db.query().fromTable('customer').join(
              db.query().fromTable('orders'),
              method='block-nested-loops',
              expr='C_CUSTKEY == O_CUSTKEY',
              ).join(
              db.query().fromTable('lineitem'),
              method='block-nested-loops',
              expr='O_ORDERKEY == L_ORDERKEY',
              ).where(
              "C_MKTSEGMENT == 'BUILDING' and O_ORDERDATE < 19950315 and L_SHIPDATE > 19950315").groupBy(
              groupSchema=groupKeySchema,
              aggSchema=groupAggSchema,
              groupExpr=(lambda e: (e.L_ORDERKEY, e.O_ORDERDATE, e.O_SHIPPRIORITY)),
              aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)],
              groupHashFn=(lambda gbVal: hash(gbVal) % 10)).select(
              {'l_orderkey' : ('L_ORDERKEY', 'int'),
               'revenue' : ('revenue', 'float'),
               'o_orderdate' : ('O_ORDERDATE', 'int'),
               'o_shippriority' : ('O_SHIPPRIORITY', 'int')}).finalize()

  query3.sample(5.0)
  print (query3.explain())
  query3 = db.optimizer.optimizeQuery(query3)
  query3.sample(5.0)
  print (query3.explain())