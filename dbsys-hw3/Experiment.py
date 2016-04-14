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

  print ("Processing query1 (unoptimized...")
  start = time.time()
  q1results = [query1.schema().unpack(tup) for page in db.processQuery(query1) for tup in page[1]]
  end = time.time()
  print ("Query 1 Processing time (unoptimized): ", end - start)
  print([tup for tup in q1results])
  print ("\n")

  query1.sample(5.0)
  print (query1.explain())
  query1 = db.optimizer.optimizeQuery(query1)
  query1.sample(5.0)
  print (query1.explain())

  print ("Processing query1...")
  start = time.time()
  q1results = [query1.schema().unpack(tup) for page in db.processQuery(query1) for tup in page[1]]
  print([tup for tup in q1results])
  end = time.time()
  print ("Query 1 processing time (optimizerd): ", end - start,"\n\n\n")



  '''
  select
          sum(l_extendedprice * (1 - l_discount)) as promo_revenue
  from
          lineitem,
          part
  where
          l_partkey = p_partkey
          and l_shipdate >= 19950901
          and l_shipdate < 19951001
  '''

  ls1 = DBSchema('partkey2',[('L_PARTKEY', 'int')])
  rs1 = DBSchema('partkey1',[('P_PARTKEY', 'int')])

  groupKeySchema = DBSchema('groupKey', [('ONE', 'int')])
  groupAggSchema = DBSchema('groupBy', [('promo_revenue','float')])

  query2 = db.query().fromTable('lineitem').join(
              db.query().fromTable('part'),
              method='block-nested-loops',
              expr='L_PARTKEY == P_PARTKEY').where(
              "L_SHIPDATE >= 19950901 and L_SHIPDATE < 19951001").groupBy(
              groupSchema=groupKeySchema,
              aggSchema=groupAggSchema,
              groupExpr=(lambda e: 1),
              aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)],
              groupHashFn=(lambda gbVal: hash(gbVal) % 1)).select(
              {'promo_revenue' : ('promo_revenue','float')}).finalize()

  print ("Processing query2 (unoptimized...")
  start = time.time()
  results = [query2.schema().unpack(tup) for page in db.processQuery(query2) for tup in page[1]]
  end = time.time()
  print ("Query 2 Processing time (unoptimized): ", end - start)
  print([tup for tup in results])
  print ("\n")

  query2.sample(5.0)
  print (query2.explain())
  query2 = db.optimizer.optimizeQuery(query2)
  query2.sample(5.0)
  print (query2.explain())

  print ("Processing query2...")
  start = time.time()
  results = [query2.schema().unpack(tup) for page in db.processQuery(query2) for tup in page[1]]
  print([tup for tup in results])
  end = time.time()
  print ("Query 2 processing time (optimizerd): ", end - start,"\n\n\n")


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

  print ("Processing query3 (unoptimized...")
  start = time.time()
  results = [query3.schema().unpack(tup) for page in db.processQuery(query3) for tup in page[1]]
  end = time.time()
  print ("Query 3 Processing time (unoptimized): ", end - start)
  print([tup for tup in results])
  print ("\n")

  query3.sample(5.0)
  print (query3.explain())
  query3 = db.optimizer.optimizeQuery(query3)
  query3.sample(5.0)
  print (query3.explain())

  print ("Processing query3...")
  start = time.time()
  results = [query3.schema().unpack(tup) for page in db.processQuery(query3) for tup in page[1]]
  print([tup for tup in results])
  end = time.time()
  print ("Query 3 processing time (optimizerd): ", end - start,"\n\n\n")
