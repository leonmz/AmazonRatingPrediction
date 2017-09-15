package cs455.spark.util;

import java.util.Date;

public class TimeDifference {
    public static String calculate(Date date1,Date date2)
    {
        long difference = date2.getTime() - date1.getTime();

        long milliseconds = difference % 1000;
        long seconds = (difference/1000)%60;
        long minutes = (difference/(1000*60))%60;
        long hours = (difference/(1000*60*60));

        return "Time taken: " + hours + " hours " + minutes + " minutes " + seconds + " seconds " + milliseconds + " milliseconds.";
    }
}
