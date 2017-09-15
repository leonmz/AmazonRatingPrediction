package cs455.spark.normalization.util;

import java.io.Serializable;

// Used to store mean normalization facts
public class MeanNormalizationFact implements Serializable{
    private double total = 0;
    private long count = 0;
    private double max = 0;
    private double min = 0;

    public MeanNormalizationFact() {
        total = 0;
        count = 0;
        max = 0;
        min = 0;
    }

    public MeanNormalizationFact(double total, long count, double max, double min) {
        this.total = total;
        this.count = count;
        this.max = max;
        this.min = min;
    }

    public double getTotal() {
        return total;
    }

    public long getCount() {
        return count;
    }

    public double getMax() {
        return max;
    }

    public double getMin() {
        return min;
    }
}
