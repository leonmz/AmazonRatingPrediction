package cs455.spark.util;

import java.util.Arrays;

public class ArrayOutput {
    Double data[];

    public ArrayOutput(Double[] data) {
        this.data = data;
    }

    @Override
    public String toString() {
        String s = "";
        for (int i = 0; i < 6; i++)
            if (i == 5)
                s += (String.valueOf(data[i]));
            else
                s += (String.valueOf(data[i]) + ";");
        return s;
    }
}
