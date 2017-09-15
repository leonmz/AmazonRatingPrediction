package cs455.spark.geneticalgorithm;

import java.io.Serializable;

// Used to store the coefficient for each feature
public class Chromosome implements Serializable{
    private double[] chromosome;
    private double fitness;

    public Chromosome(boolean notNormalized) {
        chromosome = new double[Facts.CHROMO_LENGTH];

        // Random initialize each chromosome
        for (int i = 0; i < Facts.CHROMO_LENGTH; i++)
            if (notNormalized)
            {
                // Not normalized, use random number generator between 0 and 1
                chromosome[i] = Facts.randomNumberGenerator();
            }
            else
            {
                // Normalized, use random number generator between 0 and 10
                chromosome[i] = Facts.randomNumberGenerator10();
            }
        fitness = 0;
    }

    // Given chromosome must be in length Facts.CHROMO_LENGTH
    public Chromosome(double[] chromosome) {
        this.chromosome = new double[Facts.CHROMO_LENGTH];
        for (int  i = 0; i < Facts.CHROMO_LENGTH; i++)
            this.chromosome[i] = chromosome[i];
        fitness = 0;
    }

    // Deep copy the double array then return
    public double[] getChromosome() {
        double[] output = new double[Facts.CHROMO_LENGTH];
        for (int i = 0; i < Facts.CHROMO_LENGTH; i++)
            output[i] = chromosome[i];
        return output;
    }

    // Update the chromosome
    public void setChromosome(double[] chromosome) {
        this.chromosome = new double[Facts.CHROMO_LENGTH];
        for (int i = 0; i < Facts.CHROMO_LENGTH; i++)
            this.chromosome[i] = chromosome[i];
    }

    // Assign fitness
    public void setFitness(double fitness) {
        this.fitness = fitness;
    }


    public double getFitness() {
        return fitness;
    }
}
