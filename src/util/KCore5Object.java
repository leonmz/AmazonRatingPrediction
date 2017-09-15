package cs455.spark.util;

public class KCore5Object {
    private String reviewerID;
    private String itemID;
    private String reviewerName;
    private HelpfulIndex helpfulIndex;
    private String reviewText;
    private double overall;
    private String summary;
    private int unixReviewTime;
    private String reviewTime;

    public KCore5Object() {
        this.reviewerID = "";
        this.itemID = "";
        this.reviewerName = "";
        this.helpfulIndex = new HelpfulIndex();
        this.reviewText = "";
        this.overall = 0;
        this.summary = "";
        this.unixReviewTime = 0;
        this.reviewTime = "";
    }

    public KCore5Object(String reviewerID, String itemID, String reviewerName, HelpfulIndex helpfulIndex, String reviewText, double overall, String summary, int unixReviewTime, String reviewTime) {
        this.reviewerID = reviewerID;
        this.itemID = itemID;
        this.reviewerName = reviewerName;
        this.helpfulIndex = helpfulIndex;
        this.reviewText = reviewText;
        this.overall = overall;
        this.summary = summary;
        this.unixReviewTime = unixReviewTime;
        this.reviewTime = reviewTime;
    }

    public String getReviewerID() {
        return reviewerID;
    }

    public String getItemID() {
        return itemID;
    }

    public String getReviewerName() {
        return reviewerName;
    }

    public HelpfulIndex getHelpfulIndex() {
        return helpfulIndex;
    }

    public String getReviewText() {
        return reviewText;
    }

    public double getOverall() {
        return overall;
    }

    public String getSummary() {
        return summary;
    }

    public int getUnixReviewTime() {
        return unixReviewTime;
    }

    public String getReviewTime() {
        return reviewTime;
    }

    @Override
    public String toString() {
        return "KCore5Object{" +
                "reviewerID='" + reviewerID + '\'' +
                ", itemID='" + itemID + '\'' +
                ", reviewerName='" + reviewerName + '\'' +
                ", helpfulIndex=" + helpfulIndex +
                ", reviewText='" + reviewText + '\'' +
                ", overall=" + overall +
                ", summary='" + summary + '\'' +
                ", unixReviewTime=" + unixReviewTime +
                ", reviewTime='" + reviewTime + '\'' +
                '}';
    }
}
