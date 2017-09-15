package cs455.spark.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.json.JSONArray;
import org.json.JSONObject;
import scala.Tuple2;

// Prepare the data for machine learning use
public class DataPreparer {
    public static JavaRDD<Double[]> prepare(JavaSparkContext sc, String[] args)
    {
        // Read in text file
        JavaRDD<KCore5Object> records = sc.textFile(args[0]).map(
                new Function<String, KCore5Object>() {
                    @Override
                    public KCore5Object call(String s) throws Exception {
                        // Initialization
                        String reviewerID = "";
                        String itemID = "";
                        String reviewerName = "";
                        HelpfulIndex helpfulIndex;
                        String reviewText = "";
                        double overall = 0;
                        String summary = "";
                        int unixReviewTime = 0;
                        String reviewTime = "";
                        int helpfulRatings = 0;
                        int totalRatings = 0;

                        // Retrieve information using JSON
                        JSONObject jsonData = new JSONObject(s);
                        if (jsonData.has("reviewerID"))
                            reviewerID = jsonData.getString("reviewerID");
                        if (jsonData.has("asin"))
                            itemID = jsonData.getString("asin");
                        if (jsonData.has("reviewerName"))
                            reviewerName = jsonData.getString("reviewerName");
                        if (jsonData.has("helpful"))
                        {
                            JSONArray helpfulArray = jsonData.getJSONArray("helpful");
                            helpfulRatings = (Integer)helpfulArray.get(0);
                            totalRatings = (Integer)helpfulArray.get(1);
                        }
                        helpfulIndex = new HelpfulIndex(helpfulRatings, totalRatings);
                        if (jsonData.has("reviewText"))
                            reviewText = jsonData.getString("reviewText");
                        if (jsonData.has("overall"))
                            overall = jsonData.getDouble("overall");
                        if (jsonData.has("summary"))
                            summary = jsonData.getString("summary");
                        if (jsonData.has("unixReviewTime"))
                            unixReviewTime = jsonData.getInt("unixReviewTime");
                        if (jsonData.has("reviewTime"))
                            reviewTime = jsonData.getString("reviewTime");

                        // Create and return KCore5Object
                        return new KCore5Object(reviewerID, itemID, reviewerName, helpfulIndex, reviewText, overall, summary, unixReviewTime, reviewTime);
                    }
                }
        );

        // Read metadata
        JavaRDD<MetadataObject> products = sc.textFile(args[1]).map(
                new Function<String, MetadataObject>() {
                    @Override
                    public MetadataObject call(String s) throws Exception {
                        // Initialization
                        String itemID = "";
                        // If not containing price, price is -1
                        double price = -1;

                        // Decode json object
                        try{
                            JSONObject jsonData = new JSONObject(s);
                            if (jsonData.has("asin"))
                                itemID = jsonData.getString("asin");
                            if (jsonData.has("price"))
                                price = jsonData.getDouble("price");
                        }
                        catch (Exception e)
                        {
                            itemID = "Invalid";
                            price = -1;
                        }
                        return new MetadataObject(itemID, price);
                    }
                }
        );

        // Throw away products with invalid json format
        JavaRDD<MetadataObject> filteredProducts = products.filter(
                new Function<MetadataObject, Boolean>() {
                    @Override
                    public Boolean call(MetadataObject metadataObject) throws Exception {
                        return !metadataObject.getItemID().equals("Invalid");
                    }
                }
        );

        // Map each review to <user, (rating, count)> pair and reduce by the user id
        JavaPairRDD<String, Tuple2<Double, Long>> aggregateUserRatingCount = records.mapToPair(
                new PairFunction<KCore5Object, String, Tuple2<Double, Long>>() {
                    @Override
                    public Tuple2<String, Tuple2<Double, Long>> call(KCore5Object kCore5Object) throws Exception {
                        return new Tuple2<>(kCore5Object.getReviewerID(), new Tuple2<>(kCore5Object.getOverall(), (long)1));
                    }
                }
        ).reduceByKey(
                new Function2<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>>() {
                    @Override
                    public Tuple2<Double, Long> call(Tuple2<Double, Long> lhs, Tuple2<Double, Long> rhs) throws Exception {
                        return new Tuple2<>(lhs._1 + rhs._1, lhs._2 + rhs._2);
                    }
                }
        );


        // Map into average rating, **** feature 5
        JavaPairRDD<String, Double> averageUserRatings = aggregateUserRatingCount.mapValues(
                new Function<Tuple2<Double, Long>, Double>() {
                    @Override
                    public Double call(Tuple2<Double, Long> doubleLongTuple2) throws Exception {
                        return doubleLongTuple2._1/doubleLongTuple2._2;
                    }
                }
        );

        // Calculate the global average rating
        Tuple2<Double, Long> globalTotal = aggregateUserRatingCount.values().reduce(
                new Function2<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>>() {
                    @Override
                    public Tuple2<Double, Long> call(Tuple2<Double, Long> lhs, Tuple2<Double, Long> rhs) throws Exception {
                        return new Tuple2<>(lhs._1 + rhs._1, lhs._2 + rhs._2);
                    }
                }
        );
        double globalAverage = globalTotal._1 / globalTotal._2;

        // Calculate coefficient for each user
        JavaPairRDD<String, Double> userCoefficients = averageUserRatings.mapValues(
                new Function<Double, Double>() {
                    @Override
                    public Double call(Double userAvgRating) throws Exception {
                        return globalAverage / userAvgRating;
                    }
                }
        );

        // Get pair of user and tuple<itemID, overall>, then inner join with userCoefficients, then update overall ratings, then aggregate ratings by item ID
        JavaPairRDD<String, Double> aggregatedItemRatings = records.mapToPair(
                new PairFunction<KCore5Object, String, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Tuple2<String, Double>> call(KCore5Object kCore5Object) throws Exception {
                        return new Tuple2<>(kCore5Object.getReviewerID(), new Tuple2<>(kCore5Object.getItemID(), kCore5Object.getOverall()));
                    }
                }
        ).join(userCoefficients).mapValues(
                new Function<Tuple2<Tuple2<String, Double>, Double>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> call(Tuple2<Tuple2<String, Double>, Double> data) throws Exception {
                        String itemID = data._1._1;
                        double overall = data._1._2;
                        double coefficient = data._2;
                        return new Tuple2<>(itemID, overall*coefficient);
                    }
                }
        ).values().mapToPair(
                new PairFunction<Tuple2<String, Double>, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(Tuple2<String, Double> data) throws Exception {
                        return new Tuple2<>(data._1, data._2);
                    }
                }
        );

        // Calculate aggregated item rating and count
        JavaPairRDD<String, Tuple2<Double, Long>> aggregatedItemRatingAndCount = aggregatedItemRatings.mapValues(
                new Function<Double, Tuple2<Double, Long>>() {
                    @Override
                    public Tuple2<Double, Long> call(Double data) throws Exception {
                        return new Tuple2<>(data, (long)1);
                    }
                }
        ).reduceByKey(
                new Function2<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>>() {
                    @Override
                    public Tuple2<Double, Long> call(Tuple2<Double, Long> lhs, Tuple2<Double, Long> rhs) throws Exception {
                        return new Tuple2<>(lhs._1 + rhs._1, lhs._2 + rhs._2);
                    }
                }
        );

        // Persist the aggregated item rating count
        aggregatedItemRatingAndCount.persist(StorageLevel.MEMORY_ONLY());

        // Calculate adjusted average rating of each item, **** Feature 3
        JavaPairRDD<String, Double> averageItemRating = aggregatedItemRatingAndCount.mapValues(
                new Function<Tuple2<Double, Long>, Double>() {
                    @Override
                    public Double call(Tuple2<Double, Long> data) throws Exception {
                        return data._1 / data._2;
                    }
                }
        );

        // Calculate the number of ratings of each item, **** Feature 1
        JavaPairRDD<String, Double> itemRatingCount = aggregatedItemRatingAndCount.mapValues(
                new Function<Tuple2<Double, Long>, Double>() {
                    @Override
                    public Double call(Tuple2<Double, Long> data) throws Exception {
                        return (double)data._2;
                    }
                }
        );

        // Calculate the average actual ratings of each item, ****feature 2
        JavaPairRDD<String, Double> actualItemAverageRating = records.mapToPair(
                new PairFunction<KCore5Object, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(KCore5Object kCore5Object) throws Exception {
                        return new Tuple2<>(kCore5Object.getItemID(), kCore5Object.getOverall());
                    }
                }
        ).mapValues(
                new Function<Double, Tuple2<Double, Long>>() {
                    @Override
                    public Tuple2<Double, Long> call(Double data) throws Exception {
                        return new Tuple2<>(data, (long)1);
                    }
                }
        ).reduceByKey(
                new Function2<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>>() {
                    @Override
                    public Tuple2<Double, Long> call(Tuple2<Double, Long> lhs, Tuple2<Double, Long> rhs) throws Exception {
                        return new Tuple2<>(lhs._1 + rhs._1, lhs._2 + rhs._2);
                    }
                }
        ).mapValues(
                new Function<Tuple2<Double, Long>, Double>() {
                    @Override
                    public Double call(Tuple2<Double, Long> data) throws Exception {
                        return data._1 / data._2;
                    }
                }
        );


        // Get itemID and price pair, **** Feature 4
        JavaPairRDD<String, Double> productPricePair = filteredProducts.mapToPair(
                new PairFunction<MetadataObject, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(MetadataObject metadataObject) throws Exception {
                        return new Tuple2<>(metadataObject.getItemID(), metadataObject.getPrice());
                    }
                }
        ).filter(
                new Function<Tuple2<String, Double>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Double> data) throws Exception {
                        return data._2 != -1;
                    }
                }
        );


        // **** Prepare for machine learning

        // Aggregate all features about item, in feature 1,2,3,4 sequence
        JavaPairRDD<String, Double[]> itemAggregation =itemRatingCount.join(actualItemAverageRating).join(averageItemRating.join(productPricePair)).mapValues(
                new Function<Tuple2<Tuple2<Double, Double>, Tuple2<Double, Double>>, Double[]>() {
                    @Override
                    public Double[] call(Tuple2<Tuple2<Double, Double>, Tuple2<Double, Double>> data) throws Exception {
                        Double[] result = new Double[4];
                        result[0] = data._1._1;
                        result[1] = data._1._2;
                        result[2] = data._2._1;
                        result[3] = data._2._2;
                        return result;
                    }
                }
        );

        // Actual machine learning input, format <combinedFeatureAndRating>
        // CombinedFeatureAndRating in the following format: Feature 1-5, Actual Rating
        JavaRDD<Double[]> mlInput = records.mapToPair(
                new PairFunction<KCore5Object, String, Tuple2<String, Double>>() {

                    // Get userID, (itemID, overall) pair
                    @Override
                    public Tuple2<String, Tuple2<String, Double>> call(KCore5Object kCore5Object) throws Exception {
                        return new Tuple2<>(kCore5Object.getReviewerID(), new Tuple2<>(kCore5Object.getItemID(), kCore5Object.getOverall()));
                    }
                }
        ).join(averageUserRatings).mapToPair(
                new PairFunction<Tuple2<String, Tuple2<Tuple2<String, Double>, Double>>, String, Tuple2<Tuple2<String, Double>, Double>>() {

                    // Convert from key is userID into itemID
                    @Override
                    public Tuple2<String, Tuple2<Tuple2<String, Double>, Double>> call(Tuple2<String, Tuple2<Tuple2<String, Double>, Double>> data) throws Exception {
                        String userID = data._1;
                        String itemID = data._2._1._1;
                        double overall = data._2._1._2;
                        double averageRating = data._2._2;
                        return new Tuple2<>(itemID, new Tuple2<>(new Tuple2<>(userID, overall), averageRating));
                    }
                }
        ).join(itemAggregation).mapValues(

                // Combine all features and actual rating
                new Function<Tuple2<Tuple2<Tuple2<String, Double>, Double>, Double[]>, Tuple2<String, Double[]>>() {
                    @Override
                    public Tuple2<String, Double[]> call(Tuple2<Tuple2<Tuple2<String, Double>, Double>, Double[]> data) throws Exception {
                        String userID = data._1._1._1;
                        double overall = data._1._1._2;
                        double averageRating = data._1._2;
                        Double[] itemFeatures = data._2;
                        Double[] combinedFeaturesAndRating = new Double[6];
                        for (int  i = 0; i < 4; i++)
                            combinedFeaturesAndRating[i] = itemFeatures[i];
                        combinedFeaturesAndRating[4] = averageRating;
                        combinedFeaturesAndRating[5] = overall;
                        return new Tuple2<>(userID, combinedFeaturesAndRating);
                    }
                }
        ).values().map(
                new Function<Tuple2<String, Double[]>, Double[]>() {
                    @Override
                    public Double[] call(Tuple2<String, Double[]> tuple) throws Exception {
                        return tuple._2;
                    }
                }
        );




//        JavaPairRDD<String, Tuple2<String, ArrayOutput>> output = mlInput.mapValues(
//                new Function<Tuple2<String, Double[]>, Tuple2<String, ArrayOutput>>() {
//                    @Override
//                    public Tuple2<String, ArrayOutput> call(Tuple2<String, Double[]> stringTuple2) throws Exception {
//                        return new Tuple2<>(stringTuple2._1, new ArrayOutput(stringTuple2._2));
//                    }
//                }
//        );

//        String output = "Reviews of product with current price: " + String.valueOf(combinedReviewAndProduct.count());
//        JavaRDD<String> lines = sc.parallelize(Arrays.asList(output));
//        JavaRDD<Double> globalAvgRDD = sc.parallelize(Arrays.asList(globalAverage));
//        globalAvgRDD.saveAsTextFile("/project/temp/globalAvg");
//        output.saveAsTextFile("/project/temp/mlInput");
//        mlInput.count();

        return mlInput;
    }
}
