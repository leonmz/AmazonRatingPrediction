package cs455.spark.normalization;

import cs455.spark.normalization.util.MeanNormalizationFact;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class Normalization {
    public static JavaRDD<Double[]> MeanNormalization(JavaRDD<Double[]> input)
    {
        // Extract total, count, max, min for each feature
        MeanNormalizationFact[] factsExtract = input.map(
                new Function<Double[], MeanNormalizationFact[]>() {
                    @Override
                    public MeanNormalizationFact[] call(Double[] doubles) throws Exception {
                        MeanNormalizationFact[] meanNormalizationFacts = new MeanNormalizationFact[5];
                        for (int i = 0; i < 5; i++)
                        {
                            meanNormalizationFacts[i] = new MeanNormalizationFact(doubles[i], 1, doubles[i], doubles[i]);
                        }
                        return meanNormalizationFacts;
                    }
                }
        ).reduce(
                new Function2<MeanNormalizationFact[], MeanNormalizationFact[], MeanNormalizationFact[]>() {
                    @Override
                    public MeanNormalizationFact[] call(MeanNormalizationFact[] lhs, MeanNormalizationFact[] rhs) throws Exception {
                        MeanNormalizationFact[] meanNormalizationFacts = new MeanNormalizationFact[5];
                        for (int  i = 0; i < 5; i++)
                        {
                            double max = lhs[i].getMax() > rhs[i].getMax() ? lhs[i].getMax() : rhs[i].getMax();
                            double min = lhs[i].getMin() < rhs[i].getMin() ? lhs[i].getMin() : rhs[i].getMin();
                            meanNormalizationFacts[i] = new MeanNormalizationFact(lhs[i].getTotal() + rhs[i].getTotal(), lhs[i].getCount() + rhs[i].getCount(), max, min);
                        }
                        return meanNormalizationFacts;
                    }
                }
        );

        double[] mean = new double[5];
        double[] range = new double[5];

        for (int i = 0; i < 5; i++)
        {
            MeanNormalizationFact currentFact = factsExtract[i];
            mean[i] = currentFact.getTotal() / currentFact.getCount();
            range[i] = currentFact.getMax() - currentFact.getMin();
        }

        JavaRDD<Double[]> normalizedInput = input.map(
                new Function<Double[], Double[]>() {
                    @Override
                    public Double[] call(Double[] original) throws Exception {
                        Double[] normalized = new Double[6];
                        for (int i = 0; i < 5; i++)
                        {
                            normalized[i] = (original[i] - mean[i])/range[i];
                        }
                        // Index 5 is actual rating, do not normalize
                        normalized[5] = original[5];
                        return normalized;
                    }
                }
        );

        return normalizedInput;
    }

    public static JavaRDD<Double[]> StddevNormalization(JavaRDD<Double[]> input)
    {
        // Extract sum and count
        Tuple2<Double[],Long[]> facts = input.map(
                new Function<Double[], Tuple2<Double[], Long[]>>() {
                    @Override
                    public Tuple2<Double[], Long[]> call(Double[] data) throws Exception {
                        Tuple2<Double[], Long[]> sumAndCount = new Tuple2<>(new Double[5], new Long[5]);
                        for (int i = 0; i < 5; i++)
                        {
                            sumAndCount._1[i] = data[i];
                            sumAndCount._2[i] = (long)1;
                        }
                        return sumAndCount;
                    }
                }
        ).reduce(
                new Function2<Tuple2<Double[], Long[]>, Tuple2<Double[], Long[]>, Tuple2<Double[], Long[]>>() {
                    @Override
                    public Tuple2<Double[], Long[]> call(Tuple2<Double[], Long[]> lhs, Tuple2<Double[], Long[]> rhs) throws Exception {
                        Tuple2<Double[], Long[]> sumAndCount = new Tuple2<>(new Double[5], new Long[5]);
                        for (int i = 0; i < 5; i++)
                        {
                            sumAndCount._1[i] = lhs._1[i] + rhs._1[i];
                            sumAndCount._2[i] = lhs._2[i] + rhs._2[i];
                        }
                        return sumAndCount;
                    }
                }
        );

        double[] mean = new double[5];
        for (int i = 0; i < 5; i++)
        {
            mean[i] = facts._1[i] / facts._2[i];
        }

        // Calculate stddev
        Double[] squaredDifferenceSum = input.map(
                new Function<Double[], Double[]>() {
                    @Override
                    public Double[] call(Double[] data) throws Exception {
                        Double[] squaredDifference = new Double[5];
                        for (int i = 0; i < 5; i++)
                            squaredDifference[i] = Math.pow(data[i] - mean[i], 2);
                        return squaredDifference;
                    }
                }
        ).reduce(
                new Function2<Double[], Double[], Double[]>() {
                    @Override
                    public Double[] call(Double[] lhs, Double[] rhs) throws Exception {
                        Double[] sum = new Double[5];
                        for (int i = 0; i < 5; i++)
                            sum[i] = lhs[i] + rhs[i];
                        return sum;
                    }
                }
        );

        double[] stddev = new double[5];
        for (int i = 0; i < 5; i++)
        {
            stddev[i] = Math.sqrt(squaredDifferenceSum[i]/facts._2[i]);
        }

        JavaRDD<Double[]> normalizedInput = input.map(
                new Function<Double[], Double[]>() {
                    @Override
                    public Double[] call(Double[] original) throws Exception {
                        Double[] normalized = new Double[6];
                        for (int i = 0; i < 5; i++)
                            normalized[i] = (original[i] - mean[i]) / stddev[i];
                        normalized[5] = original[5];
                        return normalized;
                    }
                }
        );

        return normalizedInput;
    }
}
