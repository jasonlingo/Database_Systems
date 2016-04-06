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




    # =======================================
    # Question 1
    # =======================================

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






    # partsuppSchema = db.relationSchema('partsupp')
    # supplierSchema = db.relationSchema('supplier')
    #
    # keySchemaPart = DBSchema('partKey', [('P_PARTKEY', 'int')])
    # keySchemaPartsupp1 = DBSchema('partsuppKey1', [('PS_PARTKEY', 'int')])
    # keySchemaPartsupp2 = DBSchema('partsuppKey2', [('PS_SUPPKEY', 'int')])
    # keySchemaSupplier = DBSchema('supplierKey', [('S_SUPPKEY', 'int')])
    #
    # part = db.query().fromTable('part')
    # supplier = db.query().fromTable('supplier')
    # partsupp = db.query().fromTable('partsupp')

    # join1 = part.join(
    #             partsupp,
    #             rhsSchema=partsuppSchema,
    #             method='hash',
    #             lhsHashFn='hash(P_PARTKEY) % 4', lhsKeySchema=keySchemaPart,
    #             rhsHashFn='hash(PS_PARTKEY) % 4', rhsKeySchema=keySchemaPartsupp1
    #         ).join(
    #             supplier,
    #             rhsSchema = supplierSchema,
    #             method = 'hash',
    #             lhsHashFn='hash(PS_SUPPKEY) % 4', lhsKeySchema=keySchemaPartsupp2,
    #             rhsHashFn = 'hash(S_SUPPKEY) % 4', rhsKeySchema=keySchemaSupplier
    #         ).where('PS_AVAILQTY == 1').select({'p_name': ('P_NAME', 'char(55)'), 's_name': ('S_NAME', 'char(25)')})
    #
    # join2 = part.join(
    #             partsupp,
    #             rhsSchema = partsuppSchema,
    #             method = 'hash',
    #             lhsHashFn='hash(P_PARTKEY) % 4', lhsKeySchema=keySchemaPart,
    #             rhsHashFn = 'hash(PS_PARTKEY) % 4', rhsKeySchema=keySchemaPartsupp1
    #         ).join(
    #             supplier,
    #             rhsSchema = supplierSchema,
    #             method = 'hash',
    #             lhsHashFn='hash(PS_SUPPKEY) % 4', lhsKeySchema=keySchemaPartsupp2,
    #             rhsHashFn = 'hash(S_SUPPKEY) % 4', rhsKeySchema=keySchemaSupplier
    #             ).where('PS_SUPPLYCOST < 5').select({'p_name': ('P_NAME', 'char(55)'), 's_name': ('S_NAME', 'char(25)')})
    #
    # query1 = join1.union(join2).finalize()


    # =======================================
    # Question 3
    # =======================================

    """ original SQL query
    with temp as (
    select n.n_name as nation, p.p_name as part, sum(l.l_quantity) as num
    from customer c, nation n, orders o, lineitem l, part p
    where c.c_nationkey = n.n_nationkey
      and c.c_custkey = o.o_custkey
      and o.o_orderkey = l.l_orderkey
      and l.l_partkey = p.p_partkey
    group by n.n_name, p.p_name
    )

    select nation, max(num)
    from temp
    group by nation;
    """

    # tables
    customer = db.relationSchema('customer')
    nation   = db.relationSchema('nation')
    orders   = db.relationSchema('orders')
    lineitem = db.relationSchema('lineitem')
    part     = db.relationSchema('part')

    print(customer.toString())
    print(nation.toString())
    print(orders.toString())
    print(lineitem.toString())
    print(part.toString())

    # # hash join
    # # customer join nation
    # cus_nationKey = DBSchema('cus_nationKey', [('C_NATIONKEY', 'int')])
    # nat_nationKey = DBSchema('nat_nationKey', [('N_NATIONKEY', 'int')])
    #
    # # customer join orders
    # cus_custKey   = DBSchema('cus_custKey', [('C_CUSTKEY', 'int')])
    # ord_custKey   = DBSchema('ord_custKey', [('O_CUSTKEY', 'int')])
    #
    # # orders join lineitem
    # ord_orderKey  = DBSchema('ord_orderKey', [('O_ORDERKEY', 'int')])
    # line_orderKey = DBSchema('line_orderKey', [('L_ORDERKEY', 'int')])
    #
    # # lineitem join part
    # line_partKey  = DBSchema('line_partKey', [('L_PARTKEY'), 'int'])
    # part_partKey  = DBSchema('part_partKey', [('P_PARTKEY'), 'int'])
    #
    # joinTables = db.query().fromTable(customer).join(
    #     nation,
    #     rhsSchema=db.relationSchema('nation'),
    #     method='hash',
    #     lhsHashFn='hash(C_NATIONKEY) % 4', lhsKeySchema=cus_nationKey,
    #     rhsHashFn='hash(N_NATIONKEY) % 4', rhsKeySchema=nat_nationKey
    # ).join(
    #     orders,
    #     rhsSchema=db.relationSchema('orders'),
    #     method='hash',
    #     lhsHashFn='hash(C_CUSTKEY) % 4', lhsKeySchema=cus_custKey,
    #     rhsHashFn='hash(O_CUSTKEY) % 4', rhsKeySchema=ord_custKey
    # ).join(
    #     lineitem,
    #     rhsSchema=db.relationSchema('lineitem'),
    #     method='hash',
    #     lhsHashFn='hash(O_ORDERKEY) % 4', lhsKeySchema=ord_orderKey,
    #     rhsHashFn='hash(L_ORDERKEY) % 4', rhsKeySchema=line_orderKey
    # ).join(
    #     part,
    #     rhsSchema=db.relationSchema('part'),
    #     method='hash',
    #     lhsHashFn='hash(L_PARTKEY) % 4', lhsKeySchema=line_partKey,
    #     rhsHashFn='hash(P_PARTKEY) % 4', rhsKeySchema=part_partKey
    # )
    #
    #
    # # first group by
    # groupSchema   = DBSchema('nation_part_name', [('N_NAME', 'char(55)'), ('P_NAME', 'char(55)')])
    # aggSumSchema  = DBSchema('nation_part_sum', [('num', 'int')])
    #
    # groupBy = joinTables.groupBy(
    #     groupSchema=groupSchema,
    #     aggSchema=aggSumSchema,
    #     groupExpr=(lambda e: (e.N_NAME, e.P_NAME)),
    #     aggExprs=[(0, lambda acc, e: acc + e.L_QUANTITY, lambda x: x)],
    #     groupHashFn=(lambda gbVal: hash(gbVal[0]) % 10)
    # )
    #
    # # second group by
    # groupSchema2  = DBSchema('nationMax', [('N_NAME', 'char(55)')])
    # aggMaxSchema  = DBSchema('aggMax', [('max', 'int')])
    #
    # query2 = groupBy.groupBy(
    #     groupSchema=groupSchema2,
    #     aggSchema=aggMaxSchema,
    #     groupExpr=(lambda e: e.N_NAME),
    #     aggExprs=[(0, lambda acc, e: max(acc, e.num), lambda x: x)],
    #     groupHashFn=(lambda gbVal: hash(gbVal[0]) % 10)
    # )


    # execute query
    # start = time.time()
    # result = getResult(db, query2)
    # end = time.time()
    # print("data:", len(result))
    # print("Time: ", end - start)
