import Database




def performQuest(db, query):
    return [query.schema().unpack(tup) for page in db.processQuery(query) for tup in page[1]]


if __name__=="__main__":
    db = Database.Database()


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
    query1 = db.query().fromTable('part')\
                .join(db.query().fromTable('supplier'),\
                      rhsSchema=,)\
                .join(db.query().fromTable('partsupp'),\
                      )\
                .where('ps_availqty = 1')\
             .union(\
                 db.query().fromTable('part')\
                    .join(db.query().fromTable('supplier'),\
                          )\
                    .join(db.query().fromTable('partsupp'),\
                          )\
                    .where('ps_supplycost < 5')\
                    .select()
                ).finalize()

