import struct

# Construct objects w/ fields corresponding to columns.
# Store fields using the appropriate representation:
  # TEXT => bytes
  # DATE => bytes
  # INTEGER => int
  # FLOAT => float

class Lineitem(object):
  # The format string, for use with the struct module.
  fmt = "iiiiffffss10s10s10s25s10s44s"

  # Initialize a lineitem object.
  # Arguments are strings that correspond to the columns of the tuple.
  # Feel free to use __new__ instead.
  # (e.g., if you decide to inherit from an immutable class).
  def __init__(self, *args):
    """
    :param args: is a list of strings that correspond to the columns of the table in order
    """
    self.l_orderkey       = int(args[0])    # i 4
    self.l_partkey        = int(args[1])    # i 4
    self.l_suppkey        = int(args[2])    # i 4
    self.l_linenumber     = int(args[3])    # i 4
    self.l_quantity       = float(args[4])  # f 4
    self.l_extendedprice  = float(args[5])  # f 4
    self.l_discount       = float(args[6])  # f 4
    self.l_tax            = float(args[7])  # f 4
    self.l_returnflag     = args[8]         # s 1
    self.l_linestatus     = args[9]         # s 1
    self.l_shipdate       = args[10]        # 10s
    self.l_commitdate     = args[11]        # 10s
    self.l_receiptdate    = args[12]        # 10s
    self.l_shipinstruct   = args[13]        # 25s
    self.l_shipmode       = args[14]        # 10s
    self.l_comment        = args[15]        # 44s
    self.dataset = (self.l_orderkey,
                    self.l_partkey,
                    self.l_suppkey,
                    self.l_linenumber,
                    self.l_quantity,
                    self.l_extendedprice,
                    self.l_discount,
                    self.l_tax,
                    self.l_returnflag,
                    self.l_linestatus,
                    self.l_shipdate,
                    self.l_commitdate,
                    self.l_receiptdate,
                    self.l_shipinstruct,
                    self.l_shipmode,
                    self.l_comment)

  # Pack this lineitem object into a bytes object.
  def pack(self):
    return struct.pack(self.fmt, *self.dataset)

  # Construct a lineitem object from a bytes object.
  @classmethod
  def unpack(cls, byts):
    unpacked = struct.unpack(cls.fmt, byts)
    return cls(*unpacked)

  # Return the size of the packed representation.
  # Do not change.
  @classmethod
  def byteSize(cls):
    return struct.calcsize(cls.fmt)

    
class Orders(object):
  # The format string, for use with the struct module.
  fmt = "iisf10s15s15si79s"

  # Initialize an orders object.
  # Arguments are strings that correspond to the columns of the tuple.
  # Feel free to use __new__ instead.
  # (e.g., if you decide to inherit from an immutable class).
  def __init__(self, *args):
    self.o_orderkey      = int(args[0])    # i
    self.o_custkey       = int(args[1])    # i
    self.o_orderstatus   = args[2]         # s
    self.o_totalprice    = float(args[3])  # f
    self.o_orderdate     = args[4]         # 10s
    self.o_orderpriority = args[5]         # 15s
    self.o_clerk         = args[6]         # 15s
    self.o_shippriority  = int(args[7])    # i
    self.o_comment       = args[8]         # 79s
    self.dataset = (self.o_orderkey,
                    self.o_custkey,
                    self.o_orderstatus,
                    self.o_totalprice,
                    self.o_orderdate,
                    self.o_orderpriority,
                    self.o_clerk,
                    self.o_shippriority,
                    self.o_comment)

  # Pack this orders object into a bytes object.
  def pack(self):
    return struct.pack(self.fmt, *self.dataset)

  # Construct an orders object from a bytes object.
  @classmethod
  def unpack(cls, byts):
    unpacked = struct.unpack(cls.fmt, byts)
    return cls(*unpacked)
  
  # Return the size of the packed representation.
  # Do not change.
  @classmethod
  def byteSize(cls):
    return struct.calcsize(cls.fmt)

# Return a list of 'cls' objects.
# Assuming 'cls' can be constructed from the raw string fields.
def readCsvFile(inPath, cls, delim='|'):
  lst = []
  with open(inPath, 'r') as f:
    for line in f:
      fields = line.strip().split(delim)
      lst.append(cls(*fields))
  return lst

# Write the list of objects to the file in packed form.
# Each object provides a 'pack' method for conversion to bytes.
def writeBinaryFile(outPath, lst):
  with open(outPath, 'wb') as f:
    for obj in lst:
      f.write(obj.pack())
  f.close()

# Read the binary file, and return a list of 'cls' objects.
# 'cls' provicdes 'byteSize' and 'unpack' methods for reading and conversion.
def readBinaryFile(inPath, cls):
  objs = []
  byteSize = cls.byteSize()
  with open(inPath, "rb") as f:
    content = f.read()
    contentSize = len(content)
    i = 0
    while i + byteSize <= contentSize:
      objs.append(cls.unpack(content[i:i+byteSize]))
      i += byteSize
  f.close()
  return objs
