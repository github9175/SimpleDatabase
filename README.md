# SimpleDatabase

#### Class summary:
- Tuple (maintains information about the contents of a tuple) implements Serializable
  - Fields: TupleDesc(the schema of this tuple), Field[](data), RecordId
  - Important method:
  - related class: IntField implements Field, StringField implements Field

- TupleDesc (describes the schema of a tuple) implements Serializable
  - Fields: TDItem[](fieldType, fieldName), Field[](data), RecordId
  - Important method: getSize()(The size (in bytes) of tuples)
  - related class: Type
