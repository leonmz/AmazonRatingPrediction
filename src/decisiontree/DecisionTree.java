package cs455.spark.decisiontree;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;

import java.util.HashMap;
import java.util.Map;

// Decision Tree Regression
public class DecisionTree {
    public static void run(JavaRDD<LabeledPoint> train_Data, JavaRDD<LabeledPoint> valid_Data)
    {
        // Set parameters.
        //  Empty categoricalFeaturesInfo indicates all features are continuous.
        System.out.println("\n\n********Decision Tree Regression********\n\n");
        Integer numClasses = 6;
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        String impurity = "gini";
        int maxDepth[] = {1,3,5,7,10,20};
        Integer maxBins = 32;

        // Train a DecisionTree model for classification.
        for(int i=0;i<maxDepth.length;i++) {
            final DecisionTreeModel model_dt = org.apache.spark.mllib.tree.DecisionTree.trainClassifier(train_Data, numClasses,
                    categoricalFeaturesInfo, impurity, maxDepth[i], maxBins);

            Evaluate.run(model_dt, train_Data, valid_Data);
        }
    }
}
