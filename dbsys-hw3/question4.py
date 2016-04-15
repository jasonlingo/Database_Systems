import Database, shutil, Storage, time
from Query.BushyOptimizer import BushyOptimizer
from Query.GreedyOptimizer import GreedyOptimizer

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


  ############################################################
# Join
# size
# 2
############################################################
print ("Join size 2")
query6 = db.query().fromTable('A').join( \
        db.query().fromTable('B'), \
        method='block-nested-loops', expr='b1 == a1').select({'a1': ('a1', 'int')}).finalize()

query6.sample(10.0)
print("Original query")
print(query6.explain())

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
#  q6results = [query6.schema().unpack(tup) for page in db.processQuery(query6) for tup in page[1]]
#  print([tup for tup in q6results])

############################################################
# Join
# size
# 4
############################################################
print ("Join size 4")
query6 = db.query().fromTable('A').join( \
        db.query().fromTable('B').select({'b1': ('b1', 'int')}), \
        method='block-nested-loops', expr='b1 == a1').join( \
        db.query().fromTable('C'), \
        method='block-nested-loops', expr='c1 == b1').join( \
        db.query().fromTable('D'), \
        method='block-nested-loops', expr='c1==d1').where('a1 > 0').finalize()

print ("Join size 4")
query6 = db.query().fromTable('A').join( \
        db.query().fromTable('B').select({'b1': ('b1', 'int')}), \
        method='block-nested-loops', expr='b1 == a1').join( \
        db.query().fromTable('C'), \
        method='block-nested-loops', expr='c1 == b1').join( \
        db.query().fromTable('D'), \
        method='block-nested-loops', expr='c1==d1').where('a1 > 0').finalize()

query6.sample(10.0)
print("Original query")
print(query6.explain())

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
############################################################
# Join
# size
# 6
############################################################

print ("Join size 6")
query6 = db.query().fromTable('A').join( \
        db.query().fromTable('B').select({'b1': ('b1', 'int')}), \
        method='block-nested-loops', expr='b1 == a1').join( \
        db.query().fromTable('C'), \
        method='block-nested-loops', expr='c1 == b1').join( \
        db.query().fromTable('D'), \
        method='block-nested-loops', expr='c1==d1').where('a1 > 0').finalize()

query6.sample(10.0)
print("Original query")
print(query6.explain())

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