package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    private TDItem[] TDItems;

    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        
        return new TDItemIterator();

    }

    private class TDItemIterator implements Iterator<TDItem>{
        int pos = 0;

        @Override

        public TDItem next(){

            if(!hasNext()){

                throw new NoSuchElementException();

            }

            return TDItems[pos++];

        }

        @Override
        public boolean hasNext(){

            return pos < TDItems.length;

        }
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        
        if(typeAr.length == 0){

            throw new IllegalArgumentException("typeAr must be nonEmpty!");

        } 

        if(typeAr.length != fieldAr.length){
        
            throw new IllegalArgumentException("The length of types must match the length of names!");
        
        }
        
        TDItems = new TDItem[typeAr.length];

        for(int i = 0; i < typeAr.length; i++){

            TDItems[i] = new TDItem(typeAr[i], fieldAr[i]);

        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {

        this(typeAr, new String[typeAr.length]);

    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {

        return TDItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {

        if(!isValidIndex(i)){

            throw new NoSuchElementException();

        }

        return TDItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {

        if(!isValidIndex(i)){

            throw new NoSuchElementException();

        }
        
        return TDItems[i].fieldType;
    }

    private boolean isValidIndex(int i){
        
        if(i >= 0 && i < numFields()) return true;
        
        else return false;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        
        if (name == null) {

            throw new NoSuchElementException();

        }

        String tempName;

        for(int i = 0; i < numFields(); i++){

            tempName = getFieldName(i);

            if(tempName != null && tempName.equals(name)) return i;

        }

        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
         
        int sum  = 0;

        for(int i = 0; i < numFields(); i++){

            sum += TDItems[i].fieldType.getLen();

        }

        return sum;

    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {

        int len1 = td1.numFields();

        int len2 = td2.numFields();

        Type[] typeAr = new Type[len1 + len2];

        String[] fieldAr = new String[len1 + len2];

        for(int i = 0; i < len1; i++){

            typeAr[i] = td1.getFieldType(i);

            fieldAr[i] = td1.getFieldName(i);

        }

        for(int i = 0; i < len2; i++){

            typeAr[len1 + i] = td2.getFieldType(i);

            fieldAr[len1 + i] = td2.getFieldName(i);

        }

        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        
        if(!(o instanceof TupleDesc)) return false;
        
        TupleDesc compare = (TupleDesc) o;

        if (compare.numFields() != this.numFields()) return false;

        for(int i = 0; i < numFields(); i++){

            if(!(compare.getFieldType(i).equals(this.getFieldType(i)))) return false;

        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
       
        StringBuffer sb = new StringBuffer();

        for(int i = 0; i < numFields(); i++){

            sb.append(TDItems[i].toString()).append(",");

        }

        sb.deleteCharAt(sb.length() - 1);
        
        return sb.toString();
    }
}
