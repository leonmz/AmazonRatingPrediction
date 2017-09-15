package cs455.spark.geneticalgorithm;

import cs455.spark.util.Benchmark;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

// Main class for genetic algorithm
public class GeneticAlgorithmMain implements Serializable{
    private JavaRDD<Double[]> input = null;
    private double[] maxFitnessChromosome = null;

    public GeneticAlgorithmMain(JavaRDD<Double[]> input, boolean notNormalized) {
        this.input = input;

        // Storage for all chromosomes
        ArrayList<Chromosome> population = new ArrayList<>();

        // Generate original population
        for (int i = 0; i < Facts.POP_SIZE; i++)
        {
            population.add(new Chromosome(notNormalized));
        }

        for (int i = 0; i < Facts.MAX_ALLOWABLE_GENERATIONS; i++)
        {
            // Assign fitness to each chromosome
            double totalFitness = 0;

            // Calculate fitnesses
            double fitness[] = calculateFitness(population);

            for (int j = 0; j < Facts.POP_SIZE; j++)
            {
                // Assign fitness to the chromosome
                population.get(j).setFitness(fitness[j]);

                // Add to total fitness
                totalFitness += fitness[j];
            }

            // Generate next generation
            ArrayList<Chromosome> newPopulation = new ArrayList<>();

            while (newPopulation.size() < Facts.POP_SIZE)
            {
                // Select two offsprings
                Chromosome chromosome1 = Roulette(totalFitness, population);
                Chromosome chromosome2 = Roulette(totalFitness, population);

                // Do Crossover
                CrossOver(chromosome1, chromosome2);

                // Do Mutation
                Mutate(chromosome1, notNormalized);
                Mutate(chromosome2, notNormalized);

                newPopulation.add(chromosome1);
                newPopulation.add(chromosome2);
            }

            // Replace old generation with new generation
            population = newPopulation;

//            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
//            Date date = new Date();
//
//            System.out.println("[" + dateFormat.format(date) + "] Currently completed iteration: " + (i+1));
        }

        double max_fitness = 0;
        double[] max_chromosome = null;

        double[] fitness = calculateFitness(population);

        for (int j = 0; j < Facts.POP_SIZE; j++)
        {

            if (fitness[j] > max_fitness)
            {
                max_fitness = fitness[j];
                max_chromosome = population.get(j).getChromosome();
            }
        }

        this.maxFitnessChromosome = max_chromosome;
        System.out.println("\nIn sample MAD: " + Benchmark.MAD(max_chromosome, input));
        System.out.println("In sample RMSE: " + Benchmark.RMSE(max_chromosome, input));

        // Accuracy test
        double[] difference = Benchmark.accuracyTest(max_chromosome, input);
        System.out.println("In sample training accuracy: " + (difference[4] * 100) + "%");
        System.out.print("In sample difference distribution: ");
        for (int i = 0; i < 9; i++)
            System.out.print((difference[i] * 100) + "% ");
        System.out.println("\n");
    }

    private double[] calculateFitness(ArrayList<Chromosome> Population)
    {
        // Get double[][] to present population of chromosomes
        double[][] matrixedPopulation = new double[Facts.POP_SIZE][Facts.CHROMO_LENGTH];

        for (int i = 0; i < Facts.POP_SIZE; i++)
        {
            double[] insideChromosome = Population.get(i).getChromosome();
            for (int j = 0; j < Facts.CHROMO_LENGTH; j++)
            {
                matrixedPopulation[i][j] = insideChromosome[j];
            }
        }

        // Tuple<Absolute Difference Sum Array, Count>
        Tuple2<Double[], Long> aggregatedAbsoluteErrorSumAndCount = input.map(
                new Function<Double[], Tuple2<Double[], Long>>() {
                    @Override
                    public Tuple2<Double[], Long> call(Double[] data) throws Exception {
                        Double[] featureAndLabel = data;
                        Double[] absoluteDifference = new Double[Facts.POP_SIZE];

                        // Calculate absolute difference and count for each chromosome
                        for (int i = 0; i < Facts.POP_SIZE; i++)
                        {
                            double[] currentCoefficient = matrixedPopulation[i];
                            double expectedLabel = 0;
                            for (int  j = 0; j < (Facts.CHROMO_LENGTH - 1); j++)
                                expectedLabel += currentCoefficient[j] * featureAndLabel[j];
                            // Add bias
                            expectedLabel += currentCoefficient[Facts.CHROMO_LENGTH - 1];
                            absoluteDifference[i] = Math.abs(expectedLabel - featureAndLabel[5]);
                        }

                        // Return the two arrays
                        return new Tuple2<>(absoluteDifference, (long)1);
                    }
                }
        ).reduce(
                new Function2<Tuple2<Double[], Long>, Tuple2<Double[], Long>, Tuple2<Double[], Long>>() {
                    @Override
                    public Tuple2<Double[], Long> call(Tuple2<Double[], Long> lhs, Tuple2<Double[], Long> rhs) throws Exception {
                        Double[] absoluteDifferenceSum = new Double[Facts.POP_SIZE];

                        for (int i = 0; i < Facts.POP_SIZE; i++)
                        {
                            absoluteDifferenceSum[i] = lhs._1[i] + rhs._1[i];
                        }

                        Long countSum = lhs._2 + rhs._2;

                        return new Tuple2<>(absoluteDifferenceSum, countSum);
                    }
                }
        );

        double[] fitness = new double[Facts.POP_SIZE];

        // Divide sum by count and do square root to get RMSE, fitness equals 1/RMSE
        for (int i = 0; i < Facts.POP_SIZE; i++)
        {
//            double RMSE = Math.sqrt(aggregatedSquaredErrorSumAndCount._1[i]/aggregatedSquaredErrorSumAndCount._2[i]);
            double MAD =aggregatedAbsoluteErrorSumAndCount._1[i] / aggregatedAbsoluteErrorSumAndCount._2;
            fitness[i] = 1/Math.pow(MAD, 3);
        }

        return fitness;
    }


    private Chromosome Roulette(double totalFitness, ArrayList<Chromosome> population)
    {
        // Generate a slice point
        double slice = Facts.randomNumberGenerator() * totalFitness;

        double currentFitness = 0;

        int i = 0;

        for (i = 0; i < Facts.POP_SIZE; i++)
        {
            currentFitness += population.get(i).getFitness();

            if (currentFitness >= slice)
                break;
        }

        return new Chromosome(population.get(i).getChromosome());
    }

    private void CrossOver(Chromosome chromosome1, Chromosome chromosome2)
    {
        // Only crossover on crossover rate
        if (Facts.randomNumberGenerator() < Facts.CROSSOVER_RATE)
        {
            // +1 to ensure 0 - Facts.CHROMO_LENGTH
            int crossoverPos = new Random().nextInt(Facts.CHROMO_LENGTH + 1);

            double[] oriChromo1 = chromosome1.getChromosome();
            double[] oriChromo2 = chromosome2.getChromosome();

            double[] newChromo1 = new double[Facts.CHROMO_LENGTH];
            double[] newChromo2 = new double[Facts.CHROMO_LENGTH];

            // Copy first part of chromosome
            for (int i = 0; i < crossoverPos; i++)
            {
                newChromo1[i] = oriChromo1[i];
                newChromo2[i] = oriChromo2[i];
            }

            // Copy second part of chromosome
            for (int i = crossoverPos; i < Facts.CHROMO_LENGTH; i++)
            {
                newChromo1[i] = oriChromo2[i];
                newChromo2[i] = oriChromo1[i];
            }

            // Update chromosome
            chromosome1.setChromosome(newChromo1);
            chromosome2.setChromosome(newChromo2);
        }
    }

    private void Mutate(Chromosome chromosome, boolean notNormalized)
    {
        double[]  insideChromosome = chromosome.getChromosome();
        for (int i = 0; i < Facts.CHROMO_LENGTH; i++)
        {
            if (Facts.randomNumberGenerator() < Facts.MUTATION_RATE)
            {
                if (notNormalized)
                    insideChromosome[i] = Facts.randomNumberGenerator();
                else
                    insideChromosome[i] = Facts.randomNumberGenerator10();
            }
        }
        chromosome.setChromosome(insideChromosome);
    }

    // Return deep copy
    public double[] getMaxFitnessChromosome() {
        double[] deepCopy = new double[maxFitnessChromosome.length];
        for (int i = 0; i < maxFitnessChromosome.length; i++)
            deepCopy[i] = maxFitnessChromosome[i];
        return deepCopy;
    }
}
