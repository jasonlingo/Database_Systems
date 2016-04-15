import Database
from Catalog.Schema import DBSchema
import time

def getResult(db, query):
    return [query.schema().unpack(tup) for page in db.processQuery(query) for tup in page[1]]


if __name__=="__main__":

  db = Database.Database(dataDir='./data')
  fileMgr = db.fileManager()
  print(fileMgr.relations())

  # print(db.relationSchema('customer').toString())
  # print(db.relationSchema('orders').toString())
  # print(db.relationSchema('lineitem').toString())
  # print(db.relationSchema('nation').toString())


  """
    select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from
        customer,
        orders,
        lineitem,
        nation
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= 19931001
        and o_orderdate < 19940101
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment

  # ======
    customer[(C_CUSTKEY,int),(C_NAME,char(25)),(C_ADDRESS,char(40)),(C_NATIONKEY,int),(C_PHONE,char(15)),(C_ACCTBAL,double),(C_MKTSEGMENT,char(10)),(C_COMMENT,char(117))]

    orders[(O_ORDERKEY,int),(O_CUSTKEY,int),(O_ORDERSTATUS,char(1)),(O_TOTALPRICE,double),(O_ORDERDATE,int),(O_ORDERPRIORITY,char(15)),(O_CLERK,char(15)),(O_SHIPPRIORITY,int),(O_COMMENT,char(79))]

    lineitem[(L_ORDERKEY,int),(L_PARTKEY,int),(L_SUPPKEY,int),(L_LINENUMBER,int),(L_QUANTITY,double),(L_EXTENDEDPRICE,double),(L_DISCOUNT,double),(L_TAX,double),(L_RETURNFLAG,char(1)),(L_LINESTATUS,char(1)),(L_SHIPDATE,int),(L_COMMITDATE,int),(L_RECEIPTDATE,int),(L_SHIPINSTRUCT,char(25)),(L_SHIPMODE,char(10)),(L_COMMENT,char(44))]

    nation[(N_NATIONKEY,int),(N_NAME,char(25)),(N_REGIONKEY,int),(N_COMMENT,char(152))]

  """

  #
  # groupKeySchema = DBSchema('groupKey', [('C_CUSTKEY', 'int'), ('C_NAME', 'char(25)'), ('C_ACCTBAL', 'double'),
  #                                        ('C_PHONE', 'char(15)'), ('N_NAME', 'char(25)'), ('C_ADDRESS', 'char(40)'),
  #                                        ('C_COMMENT', 'char(117)')])
  # groupAggSchema = DBSchema('groupAgg', [('revenue', 'double')])
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
  #              'revenue' : ('revenue', 'double'),
  #              'c_acctbal' : ('C_ACCTBAL', 'double'),
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