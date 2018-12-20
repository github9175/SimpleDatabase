# SimpleDatabase

### Path of query

parser.main() -> parser.start()

parser.start(): Database.getCatalog().loadSchema(), TableStats.computeStatistics(), processNextStatement();

processNextStatement(): handleXXXStatement(), query.execute()

handleQueryStatement(): query.setPhysicalPlan(), query.setLogicalPlan, parseQueryLogicalPlan()

parseQueryLogicalPlan(): processExpression()

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
  - Important method: insertTuple(call this.isSlotUsed, this.markSlotUsed), deleteTuple(call this.isSlotUsed, this.markSlotUsed), getPageData()
  - related class: read by HeapFile

- HeapFile implements DbFile
  - Fields: TupleDesc, file;
  - Important method: readPage(pid) from file, writePage(page) into file, insertTuple[access page through bufferpool, call page.insertTuple, page.markDirty], deleteTuple[access page through bufferpool, call page.insertTuple, page.markDirty], DbFileIterator
  - related class: DbFileIterator

- SeqScan (query all operator) implements DbIterator
  - Fields: DbFileIterator, tid 

- Predicate (compares tuples to a specified Field value) implements Serializable
  - Fields: fieldnumber, op, Fieldvalue
  - Important method: filter(Tuple t)
  
- JoinPredicate (compares fields of two tuples)
  - Fields: fieldnumber1, fieldnumber2, op
  - Important method: filter(Tuple t1, Tuple t2)
  
- Filter (an operator that implements a relational select) extends Operator
  - Fileds: Predicate, DbIterator child
  - Important method: fetchNext()
  
- Join (an operator that implements a relational select) extends Operator
  - Fileds: JoinPredicate, DbIterator child1, child2
  - Important method: fetchNext()
  - related class: Operator

- IntegerAggregator implements Aggregator
  - Fields: groupby field, aggregate field, operation
  - Important method: iterator()
  
- StringAggregator implements Aggregator
  - Fields: groupby field, aggregate field, operation
  - Important method: iterator()
  
- Aggregate extends Operator
  - Fields: DbIterator, groupby field, aggregate field, operation
  
- Insert extends Operator
  - Fields: TransactionId, DbIterator, tableid

- Delete extends Operator
  - Fields: TransactionId, DbIterator

- BufferPool
  - Fields: numPages, LRUCache, lockManager
  - Important method: getPage(), flushPage(), evictPage()

- IntHistogram
  - Fields: buckets, min, max, width, data, ntups
  - Important method: addValue(), estimateSelectivity(op, value)

- TableStats
  - Fields: HashMap<String, Histogram>()
  - Important method: estimateSelectivity()
  - Related class: Parser

- JoinOptimizer (ordering a series of joins optimally)
  - Fields: LogicalPlan p, Vector<LogicalJoinNode> joins
  - Important method: estimateJoinCost(), estimateJoinCardinality(), orderJoins(), computeCostAndCardOfSubplan()
  - Related class: LogicalPlan, LogicalJoinNode(represents a join of two tables), PlanCache(cache the best way to join a subset of the joins), CostCard(specifying the cost and cardinality of the optimal plan)
