import io, math, struct

from collections import OrderedDict
from struct      import Struct

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema      import DBSchema

import Storage.FileManager

class BufferPool:
  """
  A buffer pool implementation.

  Since the buffer pool is a cache, we do not provide any serialization methods.

  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> bp = BufferPool()
  >>> fm = Storage.FileManager.FileManager(bufferPool=bp)
  >>> bp.setFileManager(fm)

  # Check initial buffer pool size
  >>> len(bp.pool.getbuffer()) == bp.poolSize
  True

  """

  # Default to a 10 MB buffer pool.
  defaultPoolSize = 10 * (1 << 20)

  # Buffer pool constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments, with defaults if not present:
  # pageSize       : the page size to be used with this buffer pool
  # poolSize       : the size of the buffer pool
  def __init__(self, **kwargs):
    self.pageSize     = kwargs.get("pageSize", io.DEFAULT_BUFFER_SIZE)
    self.poolSize     = kwargs.get("poolSize", BufferPool.defaultPoolSize)
    self.pool         = io.BytesIO(b'\x00' * self.poolSize)

    ####################################################################################
    # DESIGN QUESTION: what other data structures do we need to keep in the buffer pool?

    self.pageMap = OrderedDict()
    self.frames = {i : None for i in range(0, self.poolSize, self.pageSize)}
    self.freeList = list(self.frames.keys())


  def setFileManager(self, fileMgr):
    self.fileMgr = fileMgr

  # Basic statistics

  def numPages(self):
    return math.floor(self.poolSize / self.pageSize)

  def numFreePages(self):
    return len(self.freeList)

  def size(self):
    return self.poolSize

  def freeSpace(self):
    return self.numFreePages() * self.pageSize

  def usedSpace(self):
    return self.size() - self.freeSpace()


  # Buffer pool operations

  def hasPage(self, pageId):
    return pageId in self.pageMap
    #raise NotImplementedError

  def getPage(self, pageId):
    if not self.hasPage(pageId):
      if self.freeList:
        frameId = self.freeList.pop(0)
        buffer = self.pool.getbuffer()[frameId:frameId + self.pageSize]
        page = self.fileMgr.readPage(pageId, buffer)
        #self.frames[pageOffset] = page
        self.pageMap[pageId] = page
        return page
      else:
        self.evictPage()
        frameId = self.freeList[0]
        buffer = self.pool.getbuffer()[frameId:frameId + self.pageSize]
        page = self.fileMgr.readPage(pageId, buffer)

        self.frames[frameId] = page

        self.pageMap[pageId] = page
        return page

    else:
      self.pageMap.move_to_end(pageId)
      return self.pageMap[pageId]
    #raise NotImplementedError

  # Removes a page from the page map, returning it to the free
  # page list without flushing the page to the disk.
  def discardPage(self, pageId):
    self.pageMap.pop(pageId)
    # self.freeList.append(pageId)
    #raise NotImplementedError

  def flushPage(self, pageId):
    page = self.pageMap[pageId]
    self.fileMgr.writePage(page)
    #raise NotImplementedError

  # Evict using LRU policy.
  # We implement LRU through the use of an OrderedDict, and by moving pages
  # to the end of the ordering every time it is accessed through getPage()
  def evictPage(self):
    pId = next (iter (self.pageMap.keys()))
    if self.pageMap[pId].isDirty():
      self.flushPage(pId)
    #self.freeList.append()
    self.pageMap.popitem(last=False)
    #raise NotImplementedError

  # Flushes all dirty pages
  def clear(self):
    for pid in self.pageMap:
      if self.pageMap[pid].header.isDirty():
        self.flushPage(pid)
    #raise NotImplementedError

if __name__ == "__main__":
    import doctest
    doctest.testmod()