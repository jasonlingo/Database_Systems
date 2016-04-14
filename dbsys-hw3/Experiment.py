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


  # groupKeySchema = DBSchema('groupKey', [('ONE', 'int')])
  # groupAggSchema = DBSchema('groupBy', [('revenue','float')])
  # query1 = db.query().fromTable('lineitem').where(
  #             "(L_SHIPDATE >= 19940101) and (L_SHIPDATE < 19950101) and \
  #             (0.06 - 0.01 <= L_DISCOUNT <= 0.06 + 0.01) and (L_QUANTITY < 24)").groupBy(
  #             groupSchema=groupKeySchema,
  #             aggSchema=groupAggSchema,
  #             groupExpr=(lambda e: 1),
  #             aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * e.L_DISCOUNT), lambda x: x)],
  #             groupHashFn=(lambda gbVal: hash(gbVal) % 1)).select(
  #             {'revenue' : ('revenue', 'float')}).finalize()
  #
  # print ("Processing query1 (unoptimized...")
  # start = time.time()
  # q1results = [query1.schema().unpack(tup) for page in db.processQuery(query1) for tup in page[1]]
  # end = time.time()
  # print ("Query 1 Processing time (unoptimized): ", end - start)
  # print([tup for tup in q1results])
  # print ("\n")
  #
  # query1.sample(10.0)
  # print (query1.explain())
  # query1 = db.optimizer.optimizeQuery(query1)
  # query1.sample(10.0)
  # print (query1.explain())
  #
  # print ("Processing query1...")
  # start = time.time()
  # q1results = [query1.schema().unpack(tup) for page in db.processQuery(query1) for tup in page[1]]
  # print([tup for tup in q1results])
  # end = time.time()
  # print ("Query 1 processing time (optimizerd): ", end - start,"\n\n\n")
  #
  #
  #
  # '''
  # select
  #         sum(l_extendedprice * (1 - l_discount)) as promo_revenue
  # from
  #         lineitem,
  #         part
  # where
  #         l_partkey = p_partkey
  #         and l_shipdate >= 19950901
  #         and l_shipdate < 19951001
  # '''
  #
  # groupKeySchema = DBSchema('groupKey', [('ONE', 'int')])
  # groupAggSchema = DBSchema('groupBy', [('promo_revenue','float')])
  #
  # query2 = db.query().fromTable('lineitem').join(
  #             db.query().fromTable('part'),
  #             method='block-nested-loops',
  #             expr='L_PARTKEY == P_PARTKEY').where(
  #             "L_SHIPDATE >= 19950901 and L_SHIPDATE < 19951001").groupBy(
  #             groupSchema=groupKeySchema,
  #             aggSchema=groupAggSchema,
  #             groupExpr=(lambda e: 1),
  #             aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)],
  #             groupHashFn=(lambda gbVal: hash(gbVal) % 1)).select(
  #             {'promo_revenue' : ('promo_revenue','float')}).finalize()
  #
  # print ("Processing query2 (unoptimized...")
  # start = time.time()
  # results = [query2.schema().unpack(tup) for page in db.processQuery(query2) for tup in page[1]]
  # end = time.time()
  # print ("Query 2 Processing time (unoptimized): ", end - start)
  # print([tup for tup in results])
  # print ("\n")
  #
  # query2.sample(5.0)
  # print (query2.explain())
  # query2 = db.optimizer.optimizeQuery(query2)
  #
  # query2.sample(5.0)
  # print (query2.explain())
  #
  # print ("Processing query2...")
  # start = time.time()
  # results = [query2.schema().unpack(tup) for page in db.processQuery(query2) for tup in page[1]]
  # print([tup for tup in results])
  # end = time.time()
  # print ("Query 2 processing time (optimizerd): ", end - start,"\n\n\n")
  #
  #
  # """
  # select
  #         l_orderkey,
  #         sum(l_extendedprice * (1 - l_discount)) as revenue,
  #         o_orderdate,
  #         o_shippriority
  # from
  #         customer,
  #         orders,
  #         lineitem
  # where
  #         c_mktsegment = 'BUILDING'
  #         and c_custkey = o_custkey
  #         and l_orderkey = o_orderkey
  #         and o_orderdate < 19950315
  #         and l_shipdate > 19950315
  # group by
  #         l_orderkey,
  #         o_orderdate,
  #         o_shippriority
  # """
  #
  # groupKeySchema = DBSchema('groupKey', [('L_ORDERKEY', 'int'), ('O_ORDERDATE', 'int'), ('O_SHIPPRIORITY', 'int')])
  # groupAggSchema = DBSchema('groupAgg', [('revenue','float')])
  #
  # print ("Processing query3...")
  # query3 = db.query().fromTable('customer').join(
  #             db.query().fromTable('orders'),
  #             method='block-nested-loops',
  #             expr='C_CUSTKEY == O_CUSTKEY',
  #             ).join(
  #             db.query().fromTable('lineitem'),
  #             method='block-nested-loops',
  #             expr='O_ORDERKEY == L_ORDERKEY',
  #             ).where(
  #             "C_MKTSEGMENT == 'BUILDING' and O_ORDERDATE < 19950315 and L_SHIPDATE > 19950315").groupBy(
  #             groupSchema=groupKeySchema,
  #             aggSchema=groupAggSchema,
  #             groupExpr=(lambda e: (e.L_ORDERKEY, e.O_ORDERDATE, e.O_SHIPPRIORITY)),
  #             aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)],
  #             groupHashFn=(lambda gbVal: hash(gbVal) % 10)).select(
  #             {'l_orderkey' : ('L_ORDERKEY', 'int'),
  #              'revenue' : ('revenue', 'float'),
  #              'o_orderdate' : ('O_ORDERDATE', 'int'),
  #              'o_shippriority' : ('O_SHIPPRIORITY', 'int')}).finalize()
  #
  # print ("Processing query3 (unoptimized...")
  # start = time.time()
  # results = [query3.schema().unpack(tup) for page in db.processQuery(query3) for tup in page[1]]
  # end = time.time()
  # print ("Query 3 Processing time (unoptimized): ", end - start)
  # print([tup for tup in results])
  # print ("\n")
  #
  # query3.sample(5.0)
  # print (query3.explain())
  # query3 = db.optimizer.optimizeQuery(query3)
  # query3.sample(5.0)
  # print (query3.explain())
  #
  # print ("Processing query3...")
  # start = time.time()
  # results = [query3.schema().unpack(tup) for page in db.processQuery(query3) for tup in page[1]]
  # print([tup for tup in results])
  # end = time.time()
  # print ("Query 3 processing time (optimizerd): ", end - start,"\n\n\n")
  #

  # '''
  # query 4
  # select
  #         c_custkey,
  #         c_name,
  #         sum(l_extendedprice * (1 - l_discount)) as revenue,
  #         c_acctbal,
  #         n_name,
  #         c_address,
  #         c_phone,
  #         c_comment
  # from
  #         customer,
  #         orders,
  #         lineitem,
  #         nation
  # where
  #         c_custkey = o_custkey
  #         and l_orderkey = o_orderkey
  #         and o_orderdate >= 19931001
  #         and o_orderdate < 19940101
  #         and l_returnflag = 'R'
  #         and c_nationkey = n_nationkey
  # group by
  #         c_custkey,
  #         c_name,
  #         c_acctbal,
  #         c_phone,
  #         n_name,
  #         c_address,
  #         c_comment
  # '''
  #
  # groupKeySchema = DBSchema('groupKey', [('C_CUSTKEY', 'int'), ('C_NAME', 'char(25)'), ('C_ACCTBAL', 'float'),
  #                                        ('C_PHONE', 'char(15)'), ('N_NAME', 'char(25)'), ('C_ADDRESS', 'char(40)'),
  #                                        ('C_COMMENT', 'char(117)')])
  # groupAggSchema = DBSchema('groupAgg', [('revenue','float')])
  #
  # query4 = db.query().fromTable('customer').join(
  #             db.query().fromTable('orders'),
  #             method='block-nested-loops',
  #             expr='C_CUSTKEY == O_CUSTKEY').join(
  #             db.query().fromTable('lineitem'),
  #             method='block-nested-loops',
  #             expr='L_ORDERKEY == O_ORDERKEY').join(
  #             db.query().fromTable('nation'),
  #             method='block-nested-loops',
  #             expr='C_NATIONKEY == N_NATIONKEY').where(
  #             "L_RETURNFLAG == 'R' and O_ORDERDATE < 19940101 and O_ORDERDATE >= 19931001").groupBy(
  #             groupSchema=groupKeySchema,
  #             aggSchema=groupAggSchema,
  #             groupExpr=(lambda e: (e.C_CUSTKEY, e.C_NAME, e.C_ACCTBAL, e.C_PHONE, e.N_NAME, e.C_ADDRESS, e.C_COMMENT)),
  #             aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)],
  #             groupHashFn=(lambda gbVal: hash(gbVal) % 10)).select(
  #             {'c_custkey' : ('C_CUSTKEY', 'int'),
  #              'c_name' : ('C_NAME', 'char(25)'),
  #              'revenue' : ('revenue', 'float'),
  #              'c_acctbal' : ('C_ACCTBAL', 'float'),
  #              'n_name' : ('N_NAME', 'char(25)'),
  #              'c_address' : ('C_ADDRESS', 'char(40)'),
  #              'c_phone' : ('C_PHONE', 'char(15)'),
  #              'c_comment' : ('C_COMMENT', 'char(117)')}).finalize()
  #
  #
  # # print ("Processing query 4 (unoptimized...")
  # # start = time.time()
  # # results = [query4.schema().unpack(tup) for page in db.processQuery(query4) for tup in page[1]]
  # # end = time.time()
  # # print ("Query 4 Processing time (unoptimized): ", end - start)
  # # print([tup for tup in results])
  # # print ("\n")
  #
  # query4.sample(10.0)
  # print (query4.explain())
  # query4 = db.optimizer.optimizeQuery(query4)
  # query4.sample(10.0)
  # print (query4.explain())
  #
  # print ("Processing query4...")
  # start = time.time()
  # results = [query4.schema().unpack(tup) for page in db.processQuery(query4) for tup in page[1]]
  # print([tup for tup in results])
  # end = time.time()
  # print ("Query 4 processing time (optimizerd): ", end - start,"\n\n\n")

  '''
  query 5
  select
          n_name,
          sum(l_extendedprice * (1 - l_discount)) as revenue
  from
          customer,
          orders,
          lineitem,
          supplier,
          nation,
          region
  where
          c_custkey = o_custkey
          and l_orderkey = o_orderkey
          and l_suppkey = s_suppkey
          and c_nationkey = s_nationkey
          and s_nationkey = n_nationkey
          and n_regionkey = r_regionkey
          and r_name = 'ASIA'
          and o_orderdate >= 19940101
          and o_orderdate < 19950101
  group by
          n_name
  '''


  groupKeySchema = DBSchema('groupKey', [('N_NAME', 'char(25)')])
  groupAggSchema = DBSchema('groupAgg', [('revenue','float')])

  schema = db.relationSchema('custkey')
  e2schema = schema.rename('custkey2', {'id':'id2', 'age':'age2'})

  query5 = db.query().fromTable('customer').join(
              db.query().fromTable('orders'),
              method='block-nested-loops',
              expr='C_CUSTKEY == O_CUSTKEY').join(
              db.query().fromTable('lineitem'),
              method='block-nested-loops',
              expr='L_ORDERKEY == O_ORDERKEY').join(
              db.query().fromTable('supplier'),
              method='block-nested-loops',
              expr='L_SUPPKEY == S_SUPPKEY'). join(
              db.query().fromTable('nation'),
              method='block-nested-loops',
              expr='N_NATIONKEY == S_NATIONKEY').join(
              db.query().fromTable('customer'),
              rhsSchema=e2schema,
              method='block-nested-loops',
              expr='C_NATIONKEY == S_NATIONKEY').join(
              db.query().fromTable('region'),
              method='block-nested-loops',
              expr='N_REGIONKEY == R_REGIONKEY').where(
              "R_NAME == 'ASIA' and O_ORDERDATE >= 19940101 and O_ORDERDATE < 19950101").groupBy(
              groupSchema=groupKeySchema,
              aggSchema=groupAggSchema,
              groupExpr=(lambda e: e.N_NAME),
              aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)],
              groupHashFn=(lambda gbVal: hash(gbVal) % 10)).select(
              {'n_name' : ('N_NAME', 'char(25)'),
               'revenue' : ('revenue', 'float')}).finalize()

  print ("Processing query5 (unoptimized...")
  start = time.time()
  results = [query5.schema().unpack(tup) for page in db.processQuery(query5) for tup in page[1]]
  end = time.time()
  print ("Query 5 Processing time (unoptimized): ", end - start)
  print([tup for tup in results])
  print ("\n")

  # query5.sample(10.0)
  # print (query5.explain())
  # query5 = db.optimizer.optimizeQuery(query5)
  # query5.sample(10.0)
  # print (query5.explain())
  #
  # print ("Processing query 5...")
  # start = time.time()
  # results = [query5.schema().unpack(tup) for page in db.processQuery(query5) for tup in page[1]]
  # print([tup for tup in results])
  # end = time.time()
  # # print ("Query 5 processing time (optimizerd): ", end - start,"\n\n\n")