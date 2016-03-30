import Database
from Catalog.Schema import DBSchema

import sys
import unittest

import warnings
import time

class Hw2PublicTests(unittest.TestCase):
  # Utilities
  def setUp(self):
    warnings.simplefilter("ignore", ResourceWarning)



  def tearDown(self):
    # self.db.removeRelation('employee')
    self.db.close()

  def getResults(self, query):
    return [query3.schema().unpack(tup) for page in self.db.processQuery(query3) for tup in page[1]]


if __name__ == '__main__':
  #unittest.main(argv=[sys.argv[0], '-v'])
    db = Database.Database(dataDir='./data')

    groupSchema  = DBSchema('p_partKey', [('P_NAME','char(55)'),()])

    rhsSchema = db.relationSchema('lineitem')
    # lhsKeySchema = DBSchema('p_partKey', [('P_PARTKEY', 'int')])
    # rhsKeySchema = DBSchema('lineitemKey', [('L_PARTKEY', 'int')])

    aggrSchema = DBSchema('countschema',  [('count', 'int')])

    nationKeySchema = DBSchema('nationKey', [('N_NATIONKEY', 'int')])


    customerSchema = db.relationSchema('customer')
    customerKeySchema = DBSchema('customerKey', [('C_CUSTKEY','int')])

    query3 = db.query().fromTable('nation').join(
            db.query().fromTable('customer'),
            rhsSchema=customerSchema,
            method = 'hash',
            lhsHashFn='hash(C_NATIONKEY) % 4',  lhsKeySchema=nationKeySchema,
            rhsHashFn='hash(N_NATIONKEY) % 4', rhsKeySchema=customerKeySchema
    ).join(

    ).groupBy(
      groupSchema=groupSchema,
      aggSchema=aggrSchema,
      groupExpr=(lambda  e: e.P_NAME),
      aggExprs=[(0, lambda acc, e: acc+1, lambda x: x)],
      groupHashFn=(lambda gbVal: hash(gbVal[0]) % 2)
    ).finalize()



    # query3_hash = db.query().fromTable('part').join(
    #         db.query().fromTable('lineitem').where("L_RETURNFLAG == 'R'"),
    #         rhsSchema=rhsSchema,
    #         method = 'hash',
    #         lhsHashFn='hash(P_PARTKEY) % 4',  lhsKeySchema=lhsKeySchema, \
    #         rhsHashFn='hash(L_PARTKEY) % 4', rhsKeySchema=rhsKeySchema, \
    # ).groupBy(
    #   groupSchema=groupSchema,
    #   aggSchema=aggrSchema,
    #   groupExpr=(lambda  e: e.P_NAME),
    #   aggExprs=[(0, lambda acc, e: acc+1, lambda x: x)],
    #   groupHashFn=(lambda gbVal: hash(gbVal[0]) % 2)
    # ).finalize()


    start = time.time()
    for page in db.processQuery(query3):
      for tup in page[1]:
        print (query3.schema().unpack(tup), "\n")
    end = time.time()

    print ("time spent: ", end - start)
    # print (db.relationSchema("lineitem").toString());