package cs455.spark.util;

public class MetadataObject {
    private String itemID;
    private double price;

    public MetadataObject() {
        itemID = "";
        price = 0;
    }

    public MetadataObject(String itemID, double price) {
        this.itemID = itemID;
        this.price = price;
    }

    public String getItemID() {
        return itemID;
    }

    public double getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return "MetadataObject{" +
                "itemID='" + itemID + '\'' +
                ", price=" + price +
                '}';
    }
}
