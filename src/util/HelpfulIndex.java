package cs455.spark.util;

public class HelpfulIndex {
    private int helpfulRatings;
    private int totalRatings;

    public HelpfulIndex() {
        helpfulRatings = 0;
        totalRatings = 0;
    }

    public HelpfulIndex(int helpfulRatings, int totalRatings) {
        this.helpfulRatings = helpfulRatings;
        this.totalRatings = totalRatings;
    }

    public int getHelpfulRatings() {
        return helpfulRatings;
    }

    public int getTotalRatings() {
        return totalRatings;
    }

    @Override
    public String toString() {
        return "HelpfulIndex{" +
                "helpfulRatings=" + helpfulRatings +
                ", totalRatings=" + totalRatings +
                '}';
    }
}
