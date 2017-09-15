package cs455.spark.ridge;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.RidgeRegressionModel;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import scala.Tuple2;

public class Evaluate {
    public static void run(RidgeRegressionModel model, JavaRDD<LabeledPoint> train_Data, JavaRDD<LabeledPoint> valid_Data,
                                           int iteration,double stepSize){
        JavaRDD<Tuple2<Double, Double>> in_sample = train_Data.map(
                new Function<LabeledPoint, Tuple2<Double, Double>>() {
                    public Tuple2<Double, Double> call(LabeledPoint point) {
                        double prediction = model.predict(point.features());
                        return new Tuple2<>(prediction, point.label());
                    }
                }
        );

        double insample_MAD = Evaluate.CalculateMAD(in_sample);
        double insample_RMSE = Evaluate.CalculateRMSE(in_sample);
        double[] insample_accuracy = Evaluate.CalculateAccuracy(in_sample);


//        System.out.println("Attributes :    NumberofReviews\tAvgRating\tQulity\tPrice\tUser'sAveageRating\tBias");
//        System.out.println("Weight vector :" + model.weights().toString());
        System.out.println("In sample: training MAD(Iteration:" + iteration + "," + stepSize + ")=" + insample_MAD);
        System.out.println("In sample: training RMSE(Iteration:" + iteration + "," + stepSize + ")="  + insample_RMSE);
        System.out.println("In sample: training Accuracy: "+(insample_accuracy[4]*100) + "%");
        System.out.print("In sample difference distribution: ");
        for (int i = 0; i < 9; i++)
            System.out.print((insample_accuracy[i] * 100) + "% ");
        System.out.println("\n");

        JavaRDD<Tuple2<Double, Double>> out_sample = valid_Data.map(
                new Function<LabeledPoint, Tuple2<Double, Double>>() {
                    public Tuple2<Double, Double> call(LabeledPoint point) {
                        double prediction = model.predict(point.features());
                        return new Tuple2<>(prediction, point.label());
                    }
                }
        );


        double outsample_MAD = Evaluate.CalculateMAD(out_sample);
        double outsample_accuracy[] = Evaluate.CalculateAccuracy(out_sample);
        double outsample_RMSE = Evaluate.CalculateRMSE(out_sample);

        System.out.println("Outsample: training MAD(Iteration:" + iteration + "," + stepSize + ")="  + outsample_MAD);
        System.out.println("Out sample: training RMSE(Iteration:" + iteration +"," + stepSize + ")="  + outsample_RMSE);
        System.out.println("Out sample: training Accuracy: "+(outsample_accuracy[4]*100) + "%");
        System.out.print("Out sample difference distribution: ");
        for (int i = 0; i < 9; i++)
            System.out.print((outsample_accuracy[i] * 100) + "% ");
        System.out.println("\n");
    }

    public static double CalculateMAD(JavaRDD<Tuple2<Double, Double>> sample)
    {
        return new JavaDoubleRDD(sample.map(
                new Function<Tuple2<Double, Double>, Object>() {
                    public Object call(Tuple2<Double, Double> pair) {
                        double h = fixData(pair);
                        return Math.abs(h - pair._2());
                    }
                }
        ).rdd()).mean();
    }

    public static double CalculateRMSE(JavaRDD<Tuple2<Double, Double>> sample)
    {
        return Math.sqrt(new JavaDoubleRDD(sample.map(
                new Function<Tuple2<Double, Double>, Object>() {
                    public Object call(Tuple2<Double, Double> pair) {
                        double h = fixData(pair);
                        return Math.pow(h - pair._2(), 2);
                    }
                }
        ).rdd()).mean());
    }

    public static double[] CalculateAccuracy(JavaRDD<Tuple2<Double, Double>> sample)
    {
        Tuple2<Long[], Long> hitAndCount = sample.map(
                new Function<Tuple2<Double, Double>, Tuple2<Long[], Long>>() {
                    @Override
                    public Tuple2<Long[], Long> call(Tuple2<Double, Double> pair) throws Exception {
                        Long[] differenceCount = new Long[9];
                        for (int i = 0; i < 9; i++)
                            differenceCount[i] = (long)0;
                        double h = fixData(pair);
                        // Label - prediction
                        int difference = (int)(pair._2 + 1e-6) - (int)(h + 1e-6);
                        differenceCount[difference + 4] += 1;
                        return new Tuple2<>(differenceCount, (long)1);

                    }
                }
        ).reduce(
                new Function2<Tuple2<Long[], Long>, Tuple2<Long[], Long>, Tuple2<Long[], Long>>() {
                    @Override
                    public Tuple2<Long[], Long> call(Tuple2<Long[], Long> lhs, Tuple2<Long[], Long> rhs) throws Exception {
                        Long[] differenceCount = new Long[9];
                        for (int i = 0; i < 9; i++)
                            differenceCount[i] = lhs._1[i] + rhs._1[i];
                        return new Tuple2<>(differenceCount, lhs._2 + rhs._2);
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

    private static double fixData(Tuple2<Double, Double> pair)
    {
        double h = 0;
        if (pair._1 >= 5)
            h = 5.0;
        else if (pair._1 <= 1)
            h = 1.0;
        else
            h = Math.round(pair._1);
        return h;
    }

}
