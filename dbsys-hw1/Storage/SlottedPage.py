import functools, math, struct
from struct import Struct
from io     import BytesIO

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema import DBSchema
from Storage.Page import PageHeader, Page

###########################################################
# DESIGN QUESTION 1: should this inherit from PageHeader?
# If so, what methods can we reuse from the parent?
#
class SlottedPageHeader(PageHeader):
  """
  A slotted page header implementation. This should store a slot bitmap
  implemented as a memoryview on the byte buffer backing the page
  associated with this header. Additionally this header object stores
  the number of slots in the array, as well as the index of the next
  available slot.

  The binary representation of this header object is: (numSlots, nextSlot, slotBuffer)

  >>> import io
  >>> buffer = io.BytesIO(bytes(4096))
  >>> ph     = SlottedPageHeader(buffer=buffer.getbuffer(), tupleSize=16)
  >>> ph2    = SlottedPageHeader.unpack(buffer.getbuffer())
  >>> ph == ph2
  True

  ## Dirty bit tests
  >>> ph.isDirty()
  False
  >>> ph.setDirty(True)
  >>> ph.isDirty()
  True
  >>> ph.setDirty(False)
  >>> ph.isDirty()
  False

  ## Tuple count tests
  >>> ph.hasFreeTuple()
  True

  # First tuple allocated should be at the first slot.
  # Notice this is a slot index, not an offset as with contiguous pages.
  >>> ph.nextFreeTuple() == 0
  True

  >>> ph.numTuples()
  1

  >>> tuplesToTest = 10
  >>> [ph.nextFreeTuple() for i in range(0, tuplesToTest)]
  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

  >>> ph.numTuples() == tuplesToTest+1
  True

  >>> ph.hasFreeTuple()
  True

  # Check space utilization
  >>> ph.usedSpace() == (tuplesToTest+1)*ph.tupleSize
  True

  >>> ph.freeSpace() == 4096 - (ph.headerSize() + ((tuplesToTest+1) * ph.tupleSize))
  True

  >>> remainingTuples = int(ph.freeSpace() / ph.tupleSize)

  # Fill the page.
  >>> [ph.nextFreeTuple() for i in range(0, remainingTuples)] # doctest:+ELLIPSIS
  [11, 12, ...]

  >>> ph.hasFreeTuple()
  False

  # No value is returned when trying to exceed the page capacity.
  >>> ph.nextFreeTuple() == None
  True

  >>> ph.freeSpace() < ph.tupleSize
  True
  """


  def __init__(self, **kwargs):
    buffer     = kwargs.get("buffer", None)
    self.flags = kwargs.get("flags", b'\x00')

    if buffer:
      self.tupleSize = kwargs.get("tupleSize", None)
      self.pageCapacity = kwargs.get("pageCapacity", len(buffer))
      self.numSlots = kwargs.get("numSlots",
                                 math.floor((self.pageCapacity - 6) / (float)(self.tupleSize + 0.125))) # could leave more space here?
      self.nextSlot = kwargs.get("nextSlot", 0)
      unpack = kwargs.get("unpack", False)

      self.slotMap = [0 for _ in range(self.numSlots)]
      self.mapSize = math.ceil(self.numSlots/8)
      fmt = "cHHH" + "B" * self.mapSize
      self.binrepr   = struct.Struct(fmt)
      self.reprSize  = self.binrepr.size

      if not unpack:
        packed = self.pack()
        buffer[0 : self.headerSize()] = packed
      # raise NotImplementedError
    else:
      raise ValueError("No backing buffer supplied for SlottedPageHeader")

  def __eq__(self, other):
    return      (self.flags == other.flags
            and self.numSlots == other.numSlots
            and self.tupleSize == other.tupleSize
            #and self.pageCapacity == other.pageCapacity
            and self.nextSlot == other.nextSlot
            and self.slotMap == other.slotMap)
    # raise NotImplementedError

  def __hash__(self):
    return hash((self.flags, self.tupleSize, self.pageCapacity, self.numSlots, self.nextSlot, tuple(self.slotMap)))
    # raise NotImplementedError

  def headerSize(self):
    return self.reprSize

  # Flag operations.
  def flag(self, mask):
    return (ord(self.flags) & mask) > 0

  def setFlag(self, mask, set):
    if set:
      self.flags = bytes([ord(self.flags) | mask])
    else:
      self.flags = bytes([ord(self.flags) & ~mask])

  # Dirty bit accessors
  def isDirty(self):
    return self.flag(PageHeader.dirtyMask)

  def setDirty(self, dirty):
    self.setFlag(PageHeader.dirtyMask, dirty)

  def numTuples(self):
    return sum(self.slotMap)
    # raise NotImplementedError

  # Returns the space available in the page associated with this header.
  def freeSpace(self):
    return self.pageCapacity - self.headerSize() - self.usedSpace()
    # raise NotImplementedError

  # Returns the space used in the page associated with this header.
  def usedSpace(self):
    return sum(self.slotMap) * self.tupleSize
    # raise NotImplementedError


  # Slot operations....may need to refine in the future
  def offsetOfSlot(self, slot):
    return self.headerSize() + slot * self.tupleSize
    # raise NotImplementedError

  def hasSlot(self, slotIndex):
    return self.slotMap[slotIndex] == 1
    # raise NotImplementedError

  def getSlot(self, slotIndex):
    buffer = self.getbuffer()
    offset = self.headerSize() + slotIndex * self.tupleSize
    print(slotIndex, offset)
    return buffer[offset : offset + self.tupleSize]
    # raise NotImplementedError

  def setSlot(self, slotIndex):
    self.slotMap[slotIndex] = 1
    self.setDirty(True)
    # raise NotImplementedError

  def resetSlot(self, slotIndex):
    self.slotMap[slotIndex] = 0
    self.setDirty(True)
    # raise NotImplementedError

  def freeSlots(self):
    self.slotMap = [0]*len(self.slotMap)
    self.setDirty(True)
    # raise NotImplementedError

  def usedSlots(self):
    return sum(self.slotMap)
    # raise NotImplementedError

  # Tuple allocation operations.

  # Returns whether the page has any free space for a tuple.
  def hasFreeTuple(self):
    return 0 in self.slotMap
    # raise NotImplementedError

  # Returns the tupleIndex of the next free tuple.
  # This should also "allocate" the tuple, such that any subsequent call
  # does not yield the same tupleIndex.
  def nextFreeTuple(self):
    if self.nextSlot < self.numSlots:
      self.slotMap[self.nextSlot] = 1
      alloSlot = self.nextSlot
      if 0 in self.slotMap:
        self.nextSlot = self.slotMap.index(0)
      else:
        self.nextSlot = self.numSlots
      return alloSlot
    # raise NotImplementedError

  def nextTupleRange(self):
    if self.hasFreeTuple():
      tupleOffset = self.nextFreeTuple() * self.tupleSize
      tupleId = int(tupleOffset / self.tupleSize)
      offset = tupleOffset + self.headerSize()
      return tupleId, offset, offset + self.tupleSize
    # raise NotImplementedError

  # Create a binary representation of a slotted page header.
  # The binary representation should include the slot contents.
  def pack(self):
    packedHeader = SlottedPageHeader.binrepr.pack(self.flags,
                                                  self.tupleSize, self.numSlots, self.nextSlot)
    count = 1 # reset every 8 times
    bits = self.slotMap[0]

    for i in range (1, len(self.slotMap)):
      if count < 8:
        bits <<= 1
        bits += self.slotMap[i]
        count += 1
      else:
        #print ('bits ',bits)
        packedHeader += Struct("B").pack(bits)
        count = 1
        bits = self.slotMap[i]

    while count < 8:
        bits <<= 1
        count += 1

    packedHeader += Struct("B").pack(bits)
    return packedHeader
    # raise NotImplementedError

  # Create a slotted page header instance from a binary representation held in the given buffer.
  @classmethod
  def unpack(cls, buffer):
    values = Struct("cHHH").unpack_from(buffer)

    header = cls(buffer=buffer, flags=values[0], tupleSize=values[1],
                 numSlots=values[2], nextSlot=values[3], unpack=True)
    slotCount = 0
    for i in range(8, header.headerSize()):
      count = 0  # reset every 8 times
      temp = buffer[i]
      while count < 8 and slotCount < header.numSlots:
        header.slotMap[slotCount] = (temp>>(7-count)) & 1
        #temp >>= 1
        count += 1
        slotCount += 1

    # header.pageCapacity = len(buffer) - header.usedSpace() - header.headerSize()
    return header
    # raise NotImplementedError

  def getOffset(self, tupleId):
    return self.headerSize() + tupleId.tupleIndex * self.tupleSize



######################################################
# DESIGN QUESTION 2: should this inherit from Page?
# If so, what methods can we reuse from the parent?
#
class SlottedPage(Page):
  """
  A slotted page implementation.

  Slotted pages use the SlottedPageHeader class for its headers, which
  maintains a set of slots to indicate valid tuples in the page.

  A slotted page interprets the tupleIndex field in a TupleId object as
  a slot index.

  >>> from Catalog.Identifiers import FileId, PageId, TupleId
  >>> from Catalog.Schema      import DBSchema

  # Test harness setup.
  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> pId    = PageId(FileId(1), 100)
  >>> p      = SlottedPage(pageId=pId, buffer=bytes(4096), schema=schema)

  # Validate header initialization
  >>> p.header.numTuples() == 0 and p.header.usedSpace() == 0
  True

  >>> p2     = SlottedPage.unpack(pId, p.pack())
  >>> p2.header == p.header
  True

  # Create and insert a tuple
  >>> e1 = schema.instantiate(1,25)
  >>> tId = p.insertTuple(schema.pack(e1))

  >>> tId.tupleIndex
  0

  # Retrieve the previous tuple
  >>> e2 = schema.unpack(p.getTuple(tId))
  >>> e2
  employee(id=1, age=25)

  # Update the tuple.
  >>> e1 = schema.instantiate(1,28)
  >>> p.putTuple(tId, schema.pack(e1))

  # Retrieve the update
  >>> e3 = schema.unpack(p.getTuple(tId))
  >>> e3
  employee(id=1, age=28)

  # Compare tuples
  >>> e1 == e3
  True

  >>> e2 == e3
  False

  # Check number of tuples in page
  >>> p.header.numTuples() == 1
  True

  # Add some more tuples
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(10)]:
  ...    _ = p.insertTuple(tup)
  ...

  # Check number of tuples in page
  >>> p.header.numTuples()
  11

  # Test iterator
  >>> [schema.unpack(tup).age for tup in p]
  [28, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test clearing of first tuple
  >>> tId = TupleId(p.pageId, 0)
  >>> sizeBeforeClear = p.header.usedSpace()
  >>> p.clearTuple(tId)

  >>> schema.unpack(p.getTuple(tId))
  employee(id=0, age=0)

  >>> p.header.usedSpace() == sizeBeforeClear
  True

  # Check that clearTuple only affects a tuple's contents, not its presence.
  >>> [schema.unpack(tup).age for tup in p]
  [0, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test removal of first tuple
  >>> sizeBeforeRemove = p.header.usedSpace()
  >>> p.deleteTuple(tId)

  >>> [schema.unpack(tup).age for tup in p]
  [20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Check that the page's slots have tracked the deletion.
  >>> p.header.usedSpace() == (sizeBeforeRemove - p.header.tupleSize)
  True

  >>> p2     = SlottedPage.unpack(pId, p.pack())
  >>> p2.header == p.header
  True
  """

  headerClass = SlottedPageHeader

  # Slotted page constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments:
  # buffer       : a byte string of initial page contents.
  # pageId       : a PageId instance identifying this page.
  # header       : a SlottedPageHeader instance.
  # schema       : the schema for tuples to be stored in the page.
  # Also, any keyword arguments needed to construct a SlottedPageHeader.
  def __init__(self, **kwargs):
    buffer = kwargs.get("buffer", None)
    if buffer:
      BytesIO.__init__(self, buffer)
      self.pageId = kwargs.get("pageId", None)
      self.header = kwargs.get("header", None)
      self.schema = kwargs.get("schema", None)

      if self.pageId and self.header:
        self.header = self.header
      elif self.pageId:
        self.header = self.initializeHeader(**kwargs)
      else:
        raise ValueError("No page identifier provided to page constructor.")

      #raise NotImplementedError

    else:
      raise ValueError("No backing buffer provided to page constructor.")


  # Header constructor override for directory pages.
  def initializeHeader(self, **kwargs):
    schema = kwargs.get("schema", None)
    if schema:
      return SlottedPageHeader(buffer=self.getbuffer(), tupleSize=schema.size)
    else:
      raise ValueError("No schema provided when constructing a slotted page.")

  # Tuple iterator.
  def __iter__(self):
    if 1 in self.header.slotMap:
      self.iterTupleIdx = self.header.slotMap.index(1)
    else:
      self.iterTupleIdx = -1
    return self
    # raise NotImplementedError

  def __next__(self):
    if self.iterTupleIdx == -1:
      raise StopIteration

    t = self.getTuple(TupleId(self.pageId, self.iterTupleIdx))
    if t:
      start = self.iterTupleIdx + 1
      self.iterTupleIdx = -1
      for i in range(start, len(self.header.slotMap)):
        if self.header.slotMap[i] == 1:
          self.iterTupleIdx = i
          break
      return t
    else:
      raise StopIteration
    # raise NotImplementedError

  # Tuple accessor methods

  # Returns a byte string representing a packed tuple for the given tuple id.
  def getTuple(self, tupleId):
    buffer = self.getbuffer()
    offset = self.header.getOffset(tupleId)
    return buffer[offset : offset + self.header.tupleSize]
    # raise NotImplementedError

  # Updates the (packed) tuple at the given tuple id.
  def putTuple(self, tupleId, tupleData):
    buffer = self.getbuffer()
    offset = self.header.getOffset(tupleId)
    buffer[offset : offset + self.header.tupleSize] = tupleData
    self.header.setDirty(True)
    # raise NotImplementedError

  # Adds a packed tuple to the page. Returns the tuple id of the newly added tuple.
  def insertTuple(self, tupleData):
    buffer = self.getbuffer()
    values = self.header.nextTupleRange()
    if values:
      self.header.setSlot(values[0])
      buffer[values[1] : values[2]] = tupleData
      self.header.setDirty(True)
      return TupleId(self.pageId, values[0])
    # raise NotImplementedError

  # Zeroes out the contents of the tuple at the given tuple id.
  def clearTuple(self, tupleId):
    buffer = self.getbuffer()
    offset = self.header.getOffset(tupleId)
    buffer[offset : offset + self.header.tupleSize] = bytes(self.header.tupleSize)
    self.header.setDirty(True)
    # raise NotImplementedError

  # Removes the tuple at the given tuple id, shifting subsequent tuples.
  def deleteTuple(self, tupleId):
    buffer = self.getbuffer()
    offset = self.header.getOffset(tupleId)
    buffer[offset : offset + self.header.tupleSize] = bytes(self.header.tupleSize)
    self.header.resetSlot(tupleId.tupleIndex)
    self.header.setDirty(True)
    # raise NotImplementedError

  # Returns a binary representation of this page.
  # This should refresh the binary representation of the page header contained
  # within the page by packing the header in place.
  def pack(self):
    buffer = self.getbuffer()
    buffer[0 : self.header.headerSize()] = self.header.pack()
    return buffer
    # raise NotImplementedError

  # Creates a Page instance from the binary representation held in the buffer.
  # The pageId of the newly constructed Page instance is given as an argument.
  @classmethod
  def unpack(cls, pageId, buffer):
    header = SlottedPageHeader.unpack(buffer)
    return cls(pageId=pageId , header=header, buffer=buffer)
    # return super().unpack(cls, pageId, buffer)
    # raise NotImplementedError


if __name__ == "__main__":
    import doctest
    doctest.testmod()
    """
    schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
    pId    = PageId(FileId(1), 100)
    p      = SlottedPage(pageId=pId, buffer=bytes(4096), schema=schema)
    for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(10)]:
      _ = p.insertTuple(tup)
    p2     = SlottedPage.unpack(pId, p.pack())
    print(p2.header == p.header)
    """

