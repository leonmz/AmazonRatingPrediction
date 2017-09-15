package cs455.spark.ridge;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.*;


public class RidgeRegression {
    public static void run(JavaRDD<LabeledPoint> train_Data, JavaRDD<LabeledPoint> valid_Data)
    {
        // Building the model

        System.out.println("\n\n********Ridge Regression********\n\n");
        int numIterations[] = {100, 1000, 10000};
        double stepSize[] = {0.01,0.1, 0.5, 1, 5};
        double lambda = 0;
        double fraction = 1;
        for (int i = 0; i < numIterations.length; i++) {
            for (int j = 0; j < stepSize.length; j++) {
                final RidgeRegressionModel model =
                        RidgeRegressionWithSGD.train(JavaRDD.toRDD(train_Data), numIterations[i], stepSize[j], lambda, fraction);

                Evaluate.run(model, train_Data, valid_Data, numIterations[i], stepSize[j]);

            }
        }
    }

}
