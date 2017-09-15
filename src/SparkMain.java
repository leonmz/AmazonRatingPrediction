package cs455.spark;

import cs455.spark.decisiontree.DecisionTree;
import cs455.spark.geneticalgorithm.GeneticAlgorithmMain;
import cs455.spark.normalization.Normalization;
import cs455.spark.ridge.RegressionPreparer;
import cs455.spark.ridge.RidgeRegression;
import cs455.spark.util.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.storage.StorageLevel;

import java.util.Date;

import static org.apache.log4j.Logger.getRootLogger;


public class SparkMain {

    // args[0] should be kcore5, args[1] should be metadata
    public static void main(String[] args)
    {

        // Create spark context
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("CS455 Term Project"));


        Logger rootLogger = getRootLogger();
        rootLogger.setLevel(Level.ERROR);

        // Convert the data into machine learning input
        // Format <combinedFeatureAndRating>
        // CombinedFeatureAndRating in the following format: Feature 1-5, Actual Rating
        JavaRDD<Double[]> mlInput = DataPreparer.prepare(sc, args);

        mlInput.persist(StorageLevel.MEMORY_ONLY());

        System.out.println("\n\n#######Data not normalized#######\n\n");

        // Not normalized block
        {
            JavaRDD<Double[]>[] splittedRDD = mlInput.randomSplit(new double[]{0.7, 0.3});

            JavaRDD<Double[]> trainingRDD = splittedRDD[0];
            JavaRDD<Double[]> validationRDD = splittedRDD[1];

            trainingRDD.persist(StorageLevel.MEMORY_ONLY());
            validationRDD.persist(StorageLevel.MEMORY_ONLY());

            // Genetic algorithm
            System.out.println("\n\n********Genetic algorithm********\n\n");
            Date dateStart = new Date();
            GeneticAlgorithmMain geneticAlgorithmMain = new GeneticAlgorithmMain(trainingRDD,true);

            double[] max_chromosome = geneticAlgorithmMain.getMaxFitnessChromosome();

            System.out.println("Out sample MAD: " + Benchmark.MAD(max_chromosome, validationRDD));
            System.out.println("Out sample RMSE: " + Benchmark.RMSE(max_chromosome, validationRDD));

            // Accuracy test
            double[] difference = Benchmark.accuracyTest(max_chromosome, validationRDD);
            System.out.println("Out sample training accuracy: " + (difference[4] * 100) + "%");
            System.out.print("Out sample difference distribution: ");
            for (int i = 0; i < 9; i++)
                System.out.print((difference[i] * 100) + "% ");
            System.out.println("\n");
//        System.out.println("CHROM: " + max_chromosome[0] + " " + max_chromosome[1] + " " + max_chromosome[2] + " " + max_chromosome[3] + " " + max_chromosome[4] + " " + max_chromosome[5] + "\n\n\n\n\n\n\n\n");

            Date dateEnd = new Date();
            System.out.println(TimeDifference.calculate(dateStart, dateEnd));

            // Prepare data
            JavaRDD<LabeledPoint> train_Data = RegressionPreparer.prepare(trainingRDD);
            JavaRDD<LabeledPoint> validation_Data = RegressionPreparer.prepare(validationRDD);

            // Ridge regression, commented out due to super slow speed
//            RidgeRegression.run(train_Data, validation_Data);

            // Decision tree
            dateStart = new Date();
            DecisionTree.run(train_Data, validation_Data);
            dateEnd = new Date();
            System.out.println(TimeDifference.calculate(dateStart, dateEnd));

            trainingRDD.unpersist();
            validationRDD.unpersist();
        }

        System.out.println("\n\n#######Data after mean normalization#######\n\n");

        // Do mean normalization
        {
            System.out.println("Normalization time:");
            Date dateStart = new Date();
            JavaRDD<Double[]> normalizedInput = Normalization.MeanNormalization(mlInput);
            Date dateEnd = new Date();
            System.out.println(TimeDifference.calculate(dateStart, dateEnd));


            JavaRDD<Double[]>[] splittedRDD = normalizedInput.randomSplit(new double[]{0.7, 0.3});

            JavaRDD<Double[]> trainingRDD = splittedRDD[0];
            JavaRDD<Double[]> validationRDD = splittedRDD[1];

            trainingRDD.persist(StorageLevel.MEMORY_ONLY());
            validationRDD.persist(StorageLevel.MEMORY_ONLY());

            // Genetic algorithm
            System.out.println("\n\n********Genetic algorithm********\n\n");
            dateStart = new Date();
            GeneticAlgorithmMain geneticAlgorithmMain = new GeneticAlgorithmMain(trainingRDD, false);

            double[] max_chromosome = geneticAlgorithmMain.getMaxFitnessChromosome();

            System.out.println("Out sample MAD: " + Benchmark.MAD(max_chromosome, validationRDD));
            System.out.println("Out sample RMSE: " + Benchmark.RMSE(max_chromosome, validationRDD));

            // Accuracy test
            double[] difference = Benchmark.accuracyTest(max_chromosome, validationRDD);
            System.out.println("Out sample training accuracy: " + (difference[4] * 100) + "%");
            System.out.print("Out sample difference distribution: ");
            for (int i = 0; i < 9; i++)
                System.out.print((difference[i] * 100) + "% ");
            System.out.println("\n");
            dateEnd = new Date();
            System.out.println(TimeDifference.calculate(dateStart, dateEnd));
//        System.out.println("CHROM: " + max_chromosome[0] + " " + max_chromosome[1] + " " + max_chromosome[2] + " " + max_chromosome[3] + " " + max_chromosome[4] + " " + max_chromosome[5] + "\n\n\n\n\n\n\n\n");

            // Prepare data
            JavaRDD<LabeledPoint> train_Data = RegressionPreparer.prepare(trainingRDD);
            JavaRDD<LabeledPoint> validation_Data = RegressionPreparer.prepare(validationRDD);

            // Ridge regression
            dateStart = new Date();
            RidgeRegression.run(train_Data, validation_Data);
            dateEnd = new Date();
            System.out.println(TimeDifference.calculate(dateStart, dateEnd));

            // Decision tree
            dateStart = new Date();
            DecisionTree.run(train_Data, validation_Data);
            dateEnd = new Date();
            System.out.println(TimeDifference.calculate(dateStart, dateEnd));

            trainingRDD.unpersist();
            validationRDD.unpersist();
        }

        System.out.println("\n\n#######Data after normalization with standard deviation#######\n\n");

        // Do stddev normalization
        {
            System.out.println("Normalization time:");
            Date dateStart = new Date();
            JavaRDD<Double[]> normalizedInput = Normalization.StddevNormalization(mlInput);
            Date dateEnd = new Date();
            System.out.println(TimeDifference.calculate(dateStart, dateEnd));

            mlInput.unpersist();

            JavaRDD<Double[]>[] splittedRDD = normalizedInput.randomSplit(new double[]{0.7, 0.3});

            JavaRDD<Double[]> trainingRDD = splittedRDD[0];
            JavaRDD<Double[]> validationRDD = splittedRDD[1];

            trainingRDD.persist(StorageLevel.MEMORY_ONLY());
            validationRDD.persist(StorageLevel.MEMORY_ONLY());

            // Genetic algorithm
            System.out.println("\n\n********Genetic algorithm********\n\n");
            dateStart = new Date();
            GeneticAlgorithmMain geneticAlgorithmMain = new GeneticAlgorithmMain(trainingRDD, false);

            double[] max_chromosome = geneticAlgorithmMain.getMaxFitnessChromosome();

            System.out.println("Out sample MAD: " + Benchmark.MAD(max_chromosome, validationRDD));
            System.out.println("Out sample RMSE: " + Benchmark.RMSE(max_chromosome, validationRDD));

            // Accuracy test
            double[] difference = Benchmark.accuracyTest(max_chromosome, validationRDD);
            System.out.println("Out sample training accuracy: " + (difference[4] * 100) + "%");
            System.out.print("Out sample difference distribution: ");
            for (int i = 0; i < 9; i++)
                System.out.print((difference[i] * 100) + "% ");
            System.out.println("\n");
            dateEnd = new Date();
            System.out.println(TimeDifference.calculate(dateStart, dateEnd));
//        System.out.println("CHROM: " + max_chromosome[0] + " " + max_chromosome[1] + " " + max_chromosome[2] + " " + max_chromosome[3] + " " + max_chromosome[4] + " " + max_chromosome[5] + "\n\n\n\n\n\n\n\n");

            // Prepare data
            JavaRDD<LabeledPoint> train_Data = RegressionPreparer.prepare(trainingRDD);
            JavaRDD<LabeledPoint> validation_Data = RegressionPreparer.prepare(validationRDD);

            // Ridge regression
            dateStart = new Date();
            RidgeRegression.run(train_Data, validation_Data);
            dateEnd = new Date();
            System.out.println(TimeDifference.calculate(dateStart, dateEnd));

            // Decision tree
            dateStart = new Date();
            DecisionTree.run(train_Data, validation_Data);
            dateEnd = new Date();
            System.out.println(TimeDifference.calculate(dateStart, dateEnd));


            trainingRDD.unpersist();
            validationRDD.unpersist();
        }

        sc.stop();

    }


}
