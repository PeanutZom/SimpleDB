package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int buckets;
    private int min = Integer.MAX_VALUE;
    private int max = Integer.MIN_VALUE;
    private int[] counts;
    private int numTp;
    private int width;
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
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        counts = new int[buckets];
        this.min = min;
        this.max = max;
        this.buckets = buckets;
        width = (int)Math.ceil((1.0+max-min)/buckets);
    }

    public int getIndex(int v){
        return (int)((v-min)/width);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (v>max||v<min){
            throw new IllegalArgumentException(String.format("value out of bounds[)"));
        }
        counts[getIndex(v)]++;
        numTp++;
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
        int bucketIndex = getIndex(v);
        // some code goes here
        switch (op){
            case EQUALS:
                if (v>max||v<min){
                    return 0;
                }
                return (double)counts[bucketIndex]/width/numTp;
            case LESS_THAN:
                if (v>max){
                    return 1;
                }
                if (v<min){
                    return 0;
                }
                double total = 0;
                double bPart = (width*(bucketIndex+1)-v)/width;
                double bFraction = counts[bucketIndex]/numTp;
                total+= bPart*bFraction;
                for (int i=bucketIndex-1; i>=0; i--){
                    total += (double)counts[i]/numTp;
                }

                return total;
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v+1);
            case GREATER_THAN:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
            case GREATER_THAN_OR_EQ:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
            case NOT_EQUALS:
                return 1-estimateSelectivity(Predicate.Op.EQUALS, v);
            case LIKE:
                throw new UnsupportedOperationException("like is not supported yet");
        }
        return -1.0;
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
    @Override
    public String toString() {
        // some code goes here
        String str = "buckets:"+buckets+" min:"+min+" max:"+max+" num of Tuples"+numTp;
        return str;
    }
}
