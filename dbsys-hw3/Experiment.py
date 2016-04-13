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

  ls1 = DBSchema('customerKey1', [('C_CUSTKEY', 'int')])
  rs1 = DBSchema('customerKey2', [('O_CUSTKEY', 'int')])

  ls2 = DBSchema('orderKey1', [('O_ORDERKEY', 'int')])
  rs2 = DBSchema('orderkey2', [('L_ORDERKEY', 'int')])

  groupKeySchema = DBSchema('groupKey', [('L_ORDERKEY', 'int'), ('O_ORDERDATE', 'int'), ('O_SHIPPRIORITY', 'int')])
  groupAggSchema = DBSchema('groupAgg', [('revenue','float')])

  query3 = db.query().fromTable('customer').join(
              db.query().fromTable('orders'),
              method = 'hash',
              lhsHashFn = 'hash(C_CUSTKEY) % 5', lhsKeySchema = ls1,
              rhsHashFn = 'hash(O_CUSTKEY) % 5', rhsKeySchema = rs1).join(
              db.query().fromTable('lineitem'),
              method = 'hash',
              lhsHashFn = 'hash(O_ORDERKEY) % 5', lhsKeySchema = ls2,
              rhsHashFn = 'hash(L_ORDERKEY) % 5', rhsKeySchema = rs2).where(
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