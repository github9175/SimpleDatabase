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
  - related class: DbFile, Database
  
- Database (initializes the catalog, the buffer pool, and the log files)
  - Fields: Catalog, BufferPool
