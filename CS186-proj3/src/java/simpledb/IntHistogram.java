package simpledb;
import simpledb.Predicate.Op;
/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    private int buckets;

    private int min;

    private int max;

    private int[] histogram;

    private int tupleNumber;

    private int width;

    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;

        this.min = min;

        this.max = max;

        histogram = new int[buckets];

        tupleNumber = 0;

        width = (int)Math.ceil((max - min + 1)/buckets);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here

        if (v==min) {

            histogram[0]++;

        } else if (v==max){

            histogram[buckets-1]++;

        } else {

            histogram[(v - min)/width]++;
        }

        tupleNumber++;

    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        int index = (v-min)/width;
        int height;
        int left = index * width + min;
        int right = index * width + min + width -1;

        if (op==Op.EQUALS) {

            if (v<min || v>max) {

                return 0.0;

            } else {

                height = histogram[index];
                
                return (double)(height/width)/tupleNumber;
            }
        }

        if (op==Op.GREATER_THAN) {

            if (v < min) {

                return 1.0;

            }

            if (v >= max) {

                return 0.0;

            } else {

            height = histogram[index];

            double b_f = (double)height/tupleNumber;

            double b_part = (double)(right-v)/width;

            double answer = b_f*b_part;
            
            for (int i=index+1; i<histogram.length; i++) {

                int height2 = histogram[i];

                answer += (double)height2/tupleNumber;
            }

            return answer;

            }
        }

        if (op==Op.LESS_THAN) {

            if (v <= min) {

                return 0.0;

            }

            if (v > max) {

                return 1.0;

            } else {
            height = histogram[index];

            double b_f = (double)height/tupleNumber;

            double b_part = (double)(v-left)/width;

            double answer = b_f*b_part;
            
            for (int i=index-1; i>=0; i--) {

                int height2 = histogram[i];

                double b_f2 = (double)height2/tupleNumber;

                answer += b_f2;
            }

            return answer;

            }
        }
        if (op==Op.LESS_THAN_OR_EQ) {
            
            return estimateSelectivity(Op.LESS_THAN, v) + estimateSelectivity(Op.EQUALS, v);
        }

        if (op==Op.GREATER_THAN_OR_EQ) {

            return estimateSelectivity(Op.GREATER_THAN, v) + estimateSelectivity(Op.EQUALS, v);

        }

        if (op==Op.LIKE) {

            if (v<min || v>max) {

                return 0.0;

            } else {

                height = histogram[index];

                return (double)(height/width)/tupleNumber;
            }

        }

        if (op==Op.NOT_EQUALS) {

            return 1 - estimateSelectivity(Op.EQUALS, v);

        }

        return 0.0; 
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return "Minvalue: " + min + " Maxvalue: " + max + " Buckets: " + buckets + " Width: " + width + " Tuplenumber: " + tupleNumber;
    }
}
