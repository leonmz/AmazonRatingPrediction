package cs455.spark.util;

import cs455.spark.geneticalgorithm.Facts;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class Benchmark {
    public static double RMSE(double[] coefficients, JavaRDD<Double[]> data)
    {
        // Tuple<Squared Difference Sum Array, Count Array>
        Tuple2<Double, Long> aggregatedSquaredErrorSumAndCount = data.map(
                new Function<Double[], Tuple2<Double, Long>>() {
                    @Override
                    public Tuple2<Double, Long> call(Double[] data) throws Exception {
                        Double[] featureAndLabel = data;
                        double squaredDifference = 0;

                        // Calculate squared difference and count
                        double expectedLabel = 0;
                        for (int  j = 0; j < (Facts.CHROMO_LENGTH - 1); j++)
                            expectedLabel += coefficients[j] * featureAndLabel[j];
                        // Add bias
                        expectedLabel += coefficients[Facts.CHROMO_LENGTH - 1];

                        // If > 5, set to 5
                        if (expectedLabel > 5)
                            expectedLabel = 5;
                        // If < 1, set to 1
                        if (expectedLabel < 1)
                            expectedLabel = 1;

                        // Round expected label
                        expectedLabel = Math.round(expectedLabel);

                        squaredDifference = Math.pow(expectedLabel - featureAndLabel[5],2);

                        // Return the two arrays
                        return new Tuple2<>(squaredDifference, (long)1);
                    }
                }
        ).reduce(
                new Function2<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>>() {
                    @Override
                    public Tuple2<Double, Long> call(Tuple2<Double, Long> lhs, Tuple2<Double, Long> rhs) throws Exception {
                        double squaredDifferenceSum = 0;
                        long countSum = 0;

                        squaredDifferenceSum = lhs._1 + rhs._1;
                        countSum = lhs._2 + rhs._2;

                        return new Tuple2<>(squaredDifferenceSum, countSum);
                    }
                }
        );

        return Math.sqrt(aggregatedSquaredErrorSumAndCount._1/aggregatedSquaredErrorSumAndCount._2);
    }

    public static double MAD(double[] coefficients, JavaRDD<Double[]> data)
    {
        // Tuple<Absolute Difference Sum Array, Count Array>
        Tuple2<Double, Long> aggregatedAbsoluteErrorSumAndCount = data.map(
                new Function<Double[], Tuple2<Double, Long>>() {
                    @Override
                    public Tuple2<Double, Long> call(Double[] data) throws Exception {
                        Double[] featureAndLabel = data;
                        double absoluteDifference = 0;

                        // Calculate absolute difference and count for each chromosome
                        double[] currentCoefficient = coefficients;
                        double expectedLabel = 0;
                        for (int  j = 0; j < (Facts.CHROMO_LENGTH - 1); j++)
                            expectedLabel += currentCoefficient[j] * featureAndLabel[j];
                        // Add bias
                        expectedLabel += currentCoefficient[Facts.CHROMO_LENGTH - 1];

                        // If > 5, set to 5
                        if (expectedLabel > 5)
                            expectedLabel = 5;
                        // If < 1, set to 1
                        if (expectedLabel < 1)
                            expectedLabel = 1;

                        // Round expected label
                        expectedLabel = Math.round(expectedLabel);

                        // Calculate absolute difference
                        absoluteDifference = Math.abs(expectedLabel - featureAndLabel[5]);

                        // Return the two arrays
                        return new Tuple2<>(absoluteDifference, (long)1);
                    }
                }
        ).reduce(
                new Function2<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>>() {
                    @Override
                    public Tuple2<Double, Long> call(Tuple2<Double, Long> lhs, Tuple2<Double, Long> rhs) throws Exception {
                        double absoluteDifferenceSum = 0;
                        long countSum = 0;

                        absoluteDifferenceSum = lhs._1 + rhs._1;
                        countSum = lhs._2 + rhs._2;

                        return new Tuple2<>(absoluteDifferenceSum, countSum);
                    }
                }
        );

        return aggregatedAbsoluteErrorSumAndCount._1 / aggregatedAbsoluteErrorSumAndCount._2;
    }

    public static double[] accuracyTest(double[] coefficients, JavaRDD<Double[]> data)
    {
        // Tuple2<Difference Array, Count>
        Tuple2<Long[], Long> hitAndCount = data.map(
                new Function<Double[], Tuple2<Long[], Long>>() {
                    @Override
                    public Tuple2<Long[], Long> call(Double[] data) throws Exception {
                        Double[] featureAndLabel = data;
                        Long[] result = new Long[9];

                        for (int i = 0; i < result.length; i++)
                            result[i] = (long)0;

                        double[] currentCoefficient = coefficients;
                        double expectedLabel = 0;
                        for (int  j = 0; j < (Facts.CHROMO_LENGTH - 1); j++)
                            expectedLabel += currentCoefficient[j] * featureAndLabel[j];
                        // Add bias
                        expectedLabel += currentCoefficient[Facts.CHROMO_LENGTH - 1];

                        // Cut corner case
                        if (expectedLabel > 5)
                            expectedLabel = 5;
                        if (expectedLabel < 1)
                            expectedLabel = 1;

                        // difference range from -4 to 4, label - prediction
                        result[(int)(featureAndLabel[5].doubleValue() + 1e-6) - (int)(Math.round(expectedLabel) + 1e-6) + 4] += 1;

                        return new Tuple2<>(result, (long)1);
                    }
                }
        ).reduce(
                new Function2<Tuple2<Long[], Long>, Tuple2<Long[], Long>, Tuple2<Long[], Long>>() {
                    @Override
                    public Tuple2<Long[], Long> call(Tuple2<Long[], Long> lhs, Tuple2<Long[], Long> rhs) throws Exception {
                        Long[] result = new Long[9];
                        for (int i = 0; i < result.length; i++)
                            result[i] = lhs._1[i] + rhs._1[i];
                        return new Tuple2<>(result, lhs._2 + rhs._2);
                    }
                }
        );

        double[] difference = new double[9];

        // Calculate percentage for each difference
        for (int i = 0; i < 9; i++)
        {
            difference[i] = ((double)hitAndCount._1[i])/ hitAndCount._2;
        }

        return difference;
    }
}
