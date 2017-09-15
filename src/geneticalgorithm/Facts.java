package cs455.spark.geneticalgorithm;

// Used to store known facts and methods
public class Facts {

    public static double CROSSOVER_RATE = 0.7;
    public static double MUTATION_RATE = 0.1;
    public static int POP_SIZE = 1000;
    public static int CHROMO_LENGTH = 6;
    public static int MAX_ALLOWABLE_GENERATIONS = 300;

    public static double randomNumberGenerator()
    {
        return Math.random();
    }

    public static double randomNumberGenerator10()
    {
        return Math.random() * 10;
    }

}
