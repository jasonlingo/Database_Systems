import Database, shutil, Storage, time
from Query.BushyOptimizer import BushyOptimizer
from Query.GreedyOptimizer import GreedyOptimizer


if __name__=="__main__":

  db = Database.Database()
  try:
    db.createRelation('A', [('a1', 'int'), ('a2', 'int'), ('a3', 'int')])
  except ValueError:
    pass
  try:
    db.createRelation('B', [('b1', 'int'), ('b2', 'int'), ('b3', 'int')])
  except ValueError:
    pass
  try:
    db.createRelation('C', [('c1', 'int'), ('c2', 'int'), ('c3', 'int')])
  except ValueError:
    pass
  try:
    db.createRelation('D', [('d1', 'int'), ('d2', 'int'), ('d3', 'int')])
  except ValueError:
    pass
  try:
    db.createRelation('E', [('e1', 'int'), ('e2', 'int'), ('e3', 'int')])
  except ValueError:
    pass
  try:
    db.createRelation('F', [('f1', 'int'), ('f2', 'int'), ('f3', 'int')])
  except ValueError:
    pass
  try:
    db.createRelation('G', [('g1', 'int'), ('g2', 'int'), ('g3', 'int')])
  except ValueError:
    pass
  try:
    db.createRelation('H', [('h1', 'int'), ('h2', 'int'), ('h3', 'int')])
  except ValueError:
    pass

  schema = db.relationSchema('A')
  for tup in [schema.pack(schema.instantiate(i, 2 * i, 3 * i)) for i in range(20)]:
    _ = db.insertTuple(schema.name, tup)

  schema = db.relationSchema('B')
  for tup in [schema.pack(schema.instantiate(i, 4 * i, 3 * i)) for i in range(20)]:
    _ = db.insertTuple(schema.name, tup)

  schema = db.relationSchema('C')
  for tup in [schema.pack(schema.instantiate(i, 2 * i, 3 * i)) for i in range(20)]:
    _ = db.insertTuple(schema.name, tup)

  schema = db.relationSchema('D')
  for tup in [schema.pack(schema.instantiate(i, 2 * i, 3 * i)) for i in range(20)]:
    _ = db.insertTuple(schema.name, tup)

  schema = db.relationSchema('E')
  for tup in [schema.pack(schema.instantiate(i, 2 * i, 3 * i)) for i in range(20)]:
    _ = db.insertTuple(schema.name, tup)

  schema = db.relationSchema('F')
  for tup in [schema.pack(schema.instantiate(i, 2 * i, 3 * i)) for i in range(20)]:
    _ = db.insertTuple(schema.name, tup)

  schema = db.relationSchema('G')
  for tup in [schema.pack(schema.instantiate(i, 2 * i, 3 * i)) for i in range(20)]:
    _ = db.insertTuple(schema.name, tup)

  schema = db.relationSchema('H')
  for tup in [schema.pack(schema.instantiate(i, 2 * i, 3 * i)) for i in range(20)]:
    _ = db.insertTuple(schema.name, tup)


    ############################################################
  # Join
  # size
  # 2
  ############################################################
  # print ("Join size 2")
  # query6 = db.query().fromTable('A').join( \
  #         db.query().fromTable('B'), \
  #         method='block-nested-loops', expr='b1 == a1').select({'a1': ('a1', 'int')}).finalize()
  #
  # query6.sample(10.0)
  # print("Original query")
  # print(query6.explain(), "\n")
  #
  # print("Optimizer")
  # start = time.time()
  # query6_1 = db.optimizer.optimizeQuery(query6)
  # end = time.time()
  # print("Running time: ", end - start)
  # query6.sample(10.0)
  # print(query6_1.explain(), "\n")
  #
  # print("GreedyOptimizer")
  # db.setOptimizer(GreedyOptimizer(db=db))
  # start = time.time()
  # query6_1 = db.optimizer.optimizeQuery(query6)
  # end = time.time()
  # print("Running time: ", end - start)
  # query6.sample(10.0)
  # print(query6_1.explain(),"\n")
  #
  # print("BushyOptimizer")
  # db.setOptimizer(BushyOptimizer(db=db))
  # start = time.time()
  # query6_1 = db.optimizer.optimizeQuery(query6)
  # end = time.time()
  # print("Running time: ", end - start)
  # query6.sample(10.0)
  # print(query6_1.explain(),"\n\n")
  #
  # ############################################################
  # # Join
  # # size
  # # 4
  # ############################################################
  # print ("Join size 4")
  # query6 = db.query().fromTable('A').join(
  #         db.query().fromTable('B'),
  #         method='block-nested-loops', expr='b1 == a1').join(
  #         db.query().fromTable('C'),
  #         method='block-nested-loops', expr='c1 == b1').join(
  #         db.query().fromTable('D'),
  #         method='block-nested-loops', expr='c1==d1').select({'a1': ('a1', 'int')}).finalize()
  #
  # query6.sample(10.0)
  # print("Original query")
  # print(query6.explain(), "\n")
  #
  # print("Optimizer")
  # start = time.time()
  # query6_1 = db.optimizer.optimizeQuery(query6)
  # end = time.time()
  # print("Running time: ", end - start)
  # query6.sample(10.0)
  # print(query6_1.explain(), "\n")
  #
  # print("GreedyOptimizer")
  # db.setOptimizer(GreedyOptimizer(db=db))
  # start = time.time()
  # query6_1 = db.optimizer.optimizeQuery(query6)
  # end = time.time()
  # print("Running time: ", end - start)
  # query6.sample(10.0)
  # print(query6_1.explain(),"\n")
  #
  # print("BushyOptimizer")
  # db.setOptimizer(BushyOptimizer(db=db))
  # start = time.time()
  # query6_1 = db.optimizer.optimizeQuery(query6)
  # end = time.time()
  # print("Running time: ", end - start)
  # query6.sample(10.0)
  # print(query6_1.explain(),"\n\n")

  ############################################################
  # Join
  # size
  # 6
  ############################################################
  #
  print ("Join size 6")
  query6 = db.query().fromTable('A').join(
            db.query().fromTable('B'),
           method='block-nested-loops', expr='b1 == a1').join(
          db.query().fromTable('C'),
          method='block-nested-loops', expr='c1 == b1').join(
          db.query().fromTable('D'),
          method='block-nested-loops', expr='d1 == c1').join(
          db.query().fromTable('E'),
          method='block-nested-loops', expr='d1 == e1').join(
          db.query().fromTable('F'),
          method='block-nested-loops', expr='e1 == f1').select({'a1': ('a1', 'int')}).finalize()

  # query6.sample(10.0)
  # print("Original query")
  # print(query6.explain(), "\n")
  #
  # print("Optimizer")
  # start = time.time()
  # query6_1 = db.optimizer.optimizeQuery(query6)
  # end = time.time()
  # print("Running time: ", end - start)
  # query6.sample(10.0)
  # print(query6_1.explain(), "\n")
  #
  # print("GreedyOptimizer")
  # db.setOptimizer(GreedyOptimizer(db=db))
  # start = time.time()
  # query6_1 = db.optimizer.optimizeQuery(query6)
  # end = time.time()
  # print("Running time: ", end - start)
  # query6.sample(10.0)
  # print(query6_1.explain(),"\n")
  #
  print("BushyOptimizer")
  db.setOptimizer(BushyOptimizer(db=db))
  start = time.time()
  query6_1 = db.optimizer.optimizeQuery(query6)
  end = time.time()
  print("Running time: ", end - start)
  query6.sample(10.0)
  print(query6_1.explain(),"\n\n")

  shutil.rmtree(Storage.FileManager.FileManager.defaultDataDir)
  ############################################################
  # Join
  # size
  # 8
  ############################################################
  try:
    db.createRelation('A', [('a1', 'int'), ('a2', 'int'), ('a3', 'int')])
  except ValueError:
    pass
  try:
    db.createRelation('B', [('b1', 'int'), ('b2', 'int'), ('b3', 'int')])
  except ValueError:
    pass
  try:
    db.createRelation('C', [('c1', 'int'), ('c2', 'int'), ('c3', 'int')])
  except ValueError:
    pass
  try:
    db.createRelation('D', [('d1', 'int'), ('d2', 'int'), ('d3', 'int')])
  except ValueError:
    pass
  try:
    db.createRelation('E', [('e1', 'int'), ('e2', 'int'), ('e3', 'int')])
  except ValueError:
    pass
  try:
    db.createRelation('F', [('f1', 'int'), ('f2', 'int'), ('f3', 'int')])
  except ValueError:
    pass
  try:
    db.createRelation('G', [('g1', 'int'), ('g2', 'int'), ('g3', 'int')])
  except ValueError:
    pass
  try:
    db.createRelation('H', [('h1', 'int'), ('h2', 'int'), ('h3', 'int')])
  except ValueError:
    pass

  schema = db.relationSchema('A')
  for tup in [schema.pack(schema.instantiate(i, 2 * i, 3 * i)) for i in range(20)]:
    _ = db.insertTuple(schema.name, tup)

  schema = db.relationSchema('B')
  for tup in [schema.pack(schema.instantiate(i, 4 * i, 3 * i)) for i in range(20)]:
    _ = db.insertTuple(schema.name, tup)

  schema = db.relationSchema('C')
  for tup in [schema.pack(schema.instantiate(i, 2 * i, 3 * i)) for i in range(20)]:
    _ = db.insertTuple(schema.name, tup)

  schema = db.relationSchema('D')
  for tup in [schema.pack(schema.instantiate(i, 2 * i, 3 * i)) for i in range(20)]:
    _ = db.insertTuple(schema.name, tup)

  schema = db.relationSchema('E')
  for tup in [schema.pack(schema.instantiate(i, 2 * i, 3 * i)) for i in range(20)]:
    _ = db.insertTuple(schema.name, tup)

  schema = db.relationSchema('F')
  for tup in [schema.pack(schema.instantiate(i, 2 * i, 3 * i)) for i in range(20)]:
    _ = db.insertTuple(schema.name, tup)

  schema = db.relationSchema('G')
  for tup in [schema.pack(schema.instantiate(i, 2 * i, 3 * i)) for i in range(20)]:
    _ = db.insertTuple(schema.name, tup)

  schema = db.relationSchema('H')
  for tup in [schema.pack(schema.instantiate(i, 2 * i, 3 * i)) for i in range(20)]:
    _ = db.insertTuple(schema.name, tup)



  print ("Join size 8")
  query6 = db.query().fromTable('A').join(
            db.query().fromTable('B'),
           method='block-nested-loops', expr='b1 == a1').join(
          db.query().fromTable('C'),
          method='block-nested-loops', expr='c1 == b1').join(
          db.query().fromTable('D'),
          method='block-nested-loops', expr='d1 == c1').join(
          db.query().fromTable('E'),
          method='block-nested-loops', expr='d1 == e1').join(
          db.query().fromTable('F'),
          method='block-nested-loops', expr='e1 == f1').join(
          db.query().fromTable('G'),
          method='block-nested-loops', expr='f1 == h1').join(
            db.query().fromTable('H'),
          method='block-nested-loops', expr='e1 == f1').select({'a1': ('a1', 'int')}).finalize()

  query6.sample(10.0)
  print("Original query")
  print(query6.explain(), "\n")

  print("Optimizer")
  start = time.time()
  query6_1 = db.optimizer.optimizeQuery(query6)
  end = time.time()
  print("Running time: ", end - start)
  query6.sample(10.0)
  print(query6_1.explain(), "\n")

  print("GreedyOptimizer")
  db.setOptimizer(GreedyOptimizer(db=db))
  start = time.time()
  query6_1 = db.optimizer.optimizeQuery(query6)
  end = time.time()
  print("Running time: ", end - start)
  query6.sample(10.0)
  print(query6_1.explain(),"\n")

  print("BushyOptimizer")
  db.setOptimizer(BushyOptimizer(db=db))
  start = time.time()
  query6_1 = db.optimizer.optimizeQuery(query6)
  end = time.time()
  print("Running time: ", end - start)
  query6.sample(10.0)
  print(query6_1.explain(),"\n\n")