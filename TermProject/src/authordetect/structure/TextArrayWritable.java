package authordetect.structure;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Created by Qiu on 4/24/15.
 * The text array writable class
 */

public class TextArrayWritable extends ArrayWritable {

    public TextArrayWritable() {
        super(Text.class);
    }

    public TextArrayWritable(Text[] textArray) {
        super(Text.class, textArray);
    }

    @Override
    public String toString() {
        Writable[] values = get();

        String string = "";
        for (int i = 0; i < values.length; i++) {
            Text valText = (Text) values[i];
            String tmp = valText.toString();
            string = string.concat(tmp + "|");
        }

        return string.substring(0, string.length() - 1);
    }
}
