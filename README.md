# SimpleDatabase

### Class summary:
- Tuple (maintains information about the contents of a tuple) implements Serializable
  - Fields: TupleDesc(the schema of this tuple), Field[](data), RecordId(pageId, tuplenumber)
  - related class: IntField implements Field, StringField implements Field

- TupleDesc (describes the schema of a tuple) implements Serializable
  - Fields: TDItem[](fieldType, fieldName)
  - Important method: getSize()(The size (in bytes) of tuples)
  - related class: Type

- Catalog (keeps track of all available tables in the database and their associated schemas)
  - Fields: id2file, id2name, id2pkeyField, name2id
  - related class: contains DbFile, in Database
  
- Database (initializes the catalog, the buffer pool, and the log files)
  - Fields: Catalog, BufferPool

- HeapPageId (Unique identifier for HeapPage objects) implements PageId
  - Fields: fileId, pgNo
  
- HeapPage (stores data for one page of HeapFiles) implements Page
  - Fields: HeapPageId, header[], tuples[], tid;
  - related class: read by HeapFile

- HeapFile implements DbFile
  - Fields: TupleDesc, file;
  - Important method: readPage(pid) from file, writePage(page) into file, insertTuple[access page through bufferpool], deleteTuple[access page through bufferpool], DbFileIterator iterator
  - related class: DbFileIterator
