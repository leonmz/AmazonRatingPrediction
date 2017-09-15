package cs455.spark.decisiontree;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import scala.Tuple2;

public class Evaluate {
    public static void run(DecisionTreeModel model, JavaRDD<LabeledPoint> train_Data, JavaRDD<LabeledPoint> valid_Data){
        JavaRDD<Tuple2<Double, Double>> in_sample = train_Data.map(
                new Function<LabeledPoint, Tuple2<Double, Double>>() {
                    public Tuple2<Double, Double> call(LabeledPoint point) {
                        double prediction = model.predict(point.features());
                        return new Tuple2<>(prediction, point.label());
                    }
                }
        );

        double insample_MAD = cs455.spark.ridge.Evaluate.CalculateMAD(in_sample);
        double insample_RMSE = cs455.spark.ridge.Evaluate.CalculateRMSE(in_sample);
        double[] insample_accuracy = cs455.spark.ridge.Evaluate.CalculateAccuracy(in_sample);

        System.out.println("Tree: " + model.toString());
        System.out.println("In sample: training MAD: "+insample_MAD);
        System.out.println("In sample: training RMSE: "+ insample_RMSE);
        System.out.println("In sample: training Accuracy: "+(insample_accuracy[4] * 100) + "%");
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

        double outsample_MAD = cs455.spark.ridge.Evaluate.CalculateMAD(out_sample);
        double outsample_RMSE = cs455.spark.ridge.Evaluate.CalculateRMSE(out_sample);
        double[] outsample_accuracy = cs455.spark.ridge.Evaluate.CalculateAccuracy(out_sample);


        System.out.println("Out sample: training MAD: "+outsample_MAD);
        System.out.println("Out sample: training RMSE: "+ outsample_RMSE);
        System.out.println("Out sample: training Accuracy: "+(outsample_accuracy[4]*100) + "%");
        System.out.print("Out sample difference distribution: ");
        for (int i = 0; i < 9; i++)
            System.out.print((outsample_accuracy[i] * 100) + "% ");
        System.out.println("\n");
    }

}
