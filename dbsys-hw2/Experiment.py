import Database
from Catalog.Schema import DBSchema
import time


def getResult(db, query):
    return [query.schema().unpack(tup) for page in db.processQuery(query) for tup in page[1]]



if __name__=="__main__":

    db = Database.Database(dataDir='./data')

    """
    select p.p_name, s.s_name
    from part p, supplier s, partsupp ps
    where p.p_partkey = ps.ps_partkey
    and ps.ps_suppkey = s.s_suppkey
    and ps.ps_availqty = 1
    union all
    select p.p_name, s.s_name
    from part p, supplier s, partsupp ps
    where p.p_partkey = ps.ps_partkey
      and ps.ps_suppkey = s.s_suppkey
      and ps.ps_supplycost < 5;
    """

    """ Schema
    supplier[(S_SUPPKEY,int),(S_NAME,char(25)),(S_ADDRESS,char(40)),(S_NATIONKEY,int),(S_PHONE,char(15)),(S_ACCTBAL,double),(S_COMMENT,char(101))]

    part[(P_PARTKEY,int),(P_NAME,char(55)),(P_MFGR,char(25)),(P_BRAND,char(10)),(P_TYPE,char(25)),(P_SIZE,int),(P_CONTAINER,char(10)),(P_RETAILPRICE,double),(P_COMMENT,char(23))]

    partsupp[(PS_PARTKEY,int),(PS_SUPPKEY,int),(PS_AVAILQTY,int),(PS_SUPPLYCOST,double),(PS_COMMENT,char(199))]
    """

    # part = db.query().fromTable('part')
    # supplier = db.query().fromTable('supplier')
    # partsupp = db.query().fromTable('partsupp')
    #
    # supplierSchema = db.relationSchema("supplier")
    # partsuppSchema = db.relationSchema('partsupp')
    #
    # lhsKeySchema1 = DBSchema('pJoinKey',  [('P_PARTKEY', 'int')])
    # rhsKeySchema1 = DBSchema('psJoinKey1', [('PS_PARTKEY', 'int')])
    #
    # lhsKeySchema2 = DBSchema('psJoinKey2',  [('PS_SUPPKEY', 'int')])
    # rhsKeySchema2 = DBSchema('sJoinKey', [('S_SUPPKEY', 'int')])

    # print(db.relationSchema('supplier').toString())
    # print(db.relationSchema('part').toString())
    # print(db.relationSchema('partsupp').toString())

    # join1 = db.query().fromTable('part')\
    #             .join(db.query().fromTable('partsupp'),
    #                   rhsSchema=partsuppSchema,
    #                   method='block-nested-loops',
    #                   expr='P_PARTKEY == PS_PARTKEY'
    #             ).join(db.query().fromTable('supplier'),
    #                    rhsSchema=supplierSchema,
    #                    method='block-nested-loops'
    #                    )\
    #             .where('PS_AVAILQTY == 1')
    #
    # join2 = db.query().fromTable('part')\
    #             .join(db.query().fromTable('partsupp'),
    #                   rhsSchema=db.relationSchema('partsupp'),
    #                   method='hash',
    #                   lhsHashFn='hash(P_PARTKEY) % 4',  lhsKeySchema=partKeySchema,
    #                   rhsHashFn='hash(PS_PARTKEY) % 4', rhsKeySchema=suppKeySchema,
    #             )\
    #             .join(db.query().fromTable('supplier'),
    #                   rhsSchema=db.relationSchema('supplier'),
    #                   method='hash',
    #                   lhsHashFn='hash(PS_SUPPKEY) % 4',  lhsKeySchema=partsuppKeySchema,
    #                   rhsHashFn='hash(S_SUPPKEY) % 4', rhsKeySchema=supplierKeySchema,
    #                   )\
    #             .where('PS_SUPPLYCOST < 5')

    # query1 = join1.union(join2).select({'P_NAME': ('P_NAME', 'char(55)'), 'S_NAME': ('S_NAME', 'char(25)')}).finalize()




    # ========== block-nested join ==========
    # query1 = db.query().fromTable('part')\
    #             .join(db.query().fromTable('partsupp'),
    #                   rhsSchema=db.relationSchema('partsupp'),
    #                   method='block-nested-loops',
    #                   expr='P_PARTKEY == PS_PARTKEY'
    #             )\
    #             .join(db.query().fromTable('supplier'),
    #                   rhsSchema=db.relationSchema('supplier'),
    #                   method='block-nested-loops',
    #                   expr='PS_SUPPKEY == S_SUPPKEY'
    #                   )\
    #             .where('PS_AVAILQTY == 1')\
    #          .union(\
    #             db.query().fromTable('part')\
    #             .join(db.query().fromTable('partsupp'),
    #                   rhsSchema=db.relationSchema('partsupp'),
    #                   method='block-nested-loops',
    #                   expr='P_PARTKEY == PS_PARTKEY'
    #             )\
    #             .join(db.query().fromTable('supplier'),
    #                   rhsSchema=db.relationSchema('supplier'),
    #                   method='block-nested-loops',
    #                   expr='PS_SUPPKEY == S_SUPPKEY'
    #                   )\
    #             .where('PS_SUPPLYCOST < 5'))\
    #             .select({'P_NAME': ('P_NAME', 'char(55)'), 'S_NAME': ('S_NAME', 'char(25)')}).finalize()





    # ========== hash join ==========
    # query1 = part.join(partsupp,
    #                   rhsSchema=partsuppSchema,
    #                   method='hash',
    #                   lhsHashFn='hash(P_PARTKEY) % 4',  lhsKeySchema=lhsKeySchema1,
    #                   rhsHashFn='hash(PS_PARTKEY) % 4', rhsKeySchema=rhsKeySchema1,
    #             )\
    #             .join(supplier,
    #                   rhsSchema=supplierSchema,
    #                   method='hash',
    #                   lhsHashFn='hash(PS_SUPPKEY) % 4',  lhsKeySchema=lhsKeySchema2,
    #                   rhsHashFn='hash(S_SUPPKEY) % 4', rhsKeySchema=rhsKeySchema2,
    #             )\
    #             .where('PS_AVAILQTY == 1')\
    #             .select({'p_name': ('P_NAME', 'char(55)'), 's_name': ('S_NAME', 'char(25)')})\
    #          .union(
    #             part.join(partsupp,
    #                   rhsSchema=partsuppSchema,
    #                   method='hash',
    #                   lhsHashFn='hash(P_PARTKEY) % 4',  lhsKeySchema=lhsKeySchema1,
    #                   rhsHashFn='hash(PS_PARTKEY) % 4', rhsKeySchema=rhsKeySchema1,
    #             )\
    #             .join(supplier,
    #                   rhsSchema=supplierSchema,
    #                   method='hash',
    #                   lhsHashFn='hash(PS_SUPPKEY) % 4',  lhsKeySchema=lhsKeySchema2,
    #                   rhsHashFn='hash(S_SUPPKEY) % 4', rhsKeySchema=rhsKeySchema2,
    #                   )\
    #             .where('PS_SUPPLYCOST < 5')\
    #             .select({'p_name': ('P_NAME', 'char(55)'), 's_name': ('S_NAME', 'char(25)')}))\
    #             .finalize()





    #
    partsuppSchema = db.relationSchema('partsupp')
    supplierSchema = db.relationSchema('supplier')

    keySchemaPart = DBSchema('partKey', [('P_PARTKEY', 'int')])
    keySchemaPartsupp1 = DBSchema('partsuppKey1', [('PS_PARTKEY', 'int')])
    keySchemaPartsupp2 = DBSchema('partsuppKey2', [('PS_SUPPKEY', 'int')])
    keySchemaSupplier = DBSchema('supplierKey', [('S_SUPPKEY', 'int')])

    part = db.query().fromTable('part')
    supplier = db.query().fromTable('supplier')
    partsupp = db.query().fromTable('partsupp')

    join1 = part.join(
                partsupp,
                rhsSchema=partsuppSchema,
                method='hash',
                lhsHashFn='hash(P_PARTKEY) % 4', lhsKeySchema=keySchemaPart,
                rhsHashFn='hash(PS_PARTKEY) % 4', rhsKeySchema=keySchemaPartsupp1
            ).join(
                supplier,
                rhsSchema = supplierSchema,
                method = 'hash',
                lhsHashFn='hash(PS_SUPPKEY) % 4', lhsKeySchema=keySchemaPartsupp2,
                rhsHashFn = 'hash(S_SUPPKEY) % 4', rhsKeySchema=keySchemaSupplier
            ).where('PS_AVAILQTY == 1').select({'p_name': ('P_NAME', 'char(55)'), 's_name': ('S_NAME', 'char(25)')})

    join2 = part.join(
                partsupp,
                rhsSchema = partsuppSchema,
                method = 'hash',
                lhsHashFn='hash(P_PARTKEY) % 4', lhsKeySchema=keySchemaPart,
                rhsHashFn = 'hash(PS_PARTKEY) % 4', rhsKeySchema=keySchemaPartsupp1
            ).join(
                supplier,
                rhsSchema = supplierSchema,
                method = 'hash',
                lhsHashFn='hash(PS_SUPPKEY) % 4', lhsKeySchema=keySchemaPartsupp2,
                rhsHashFn = 'hash(S_SUPPKEY) % 4', rhsKeySchema=keySchemaSupplier
                ).where('PS_SUPPLYCOST < 5').select({'p_name': ('P_NAME', 'char(55)'), 's_name': ('S_NAME', 'char(25)')})

    query1 = join1.union(join2).finalize()



    # execute query
    start = time.time()
    result = getResult(db, query1)
    end = time.time()
    # print("data:", len(result))
    print (result)
    print("Time: ", end - start)
