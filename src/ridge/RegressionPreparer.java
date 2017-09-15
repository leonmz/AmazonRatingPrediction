package cs455.spark.ridge;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;


public class RegressionPreparer {
    public static JavaRDD<LabeledPoint> prepare(JavaRDD<Double[]> input)
    {
        JavaRDD<LabeledPoint> output = input.map(
                new Function<Double[], LabeledPoint>() {
                    @Override
                    public LabeledPoint call(Double[] input) throws Exception {
                        Double[] data = input;
                        double[] v = new double[data.length];
                        for (int i = 0; i < data.length - 1; i++) {
                            v[i] = data[i];
                        }
                        v[data.length - 1] = 1; //bias term
                        return new LabeledPoint(data[data.length - 1], Vectors.dense(v));
                    }
                }
        );
        return output;
    }
}
