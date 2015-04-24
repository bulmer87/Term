package structure;

import java.util.HashMap;

/**
 * Created by Qiu on 4/24/2015.
 * Custom Hashmap for storing word and its count
 */

public class WordCountMap extends HashMap<String, Integer> {

    /**
     * When adding an existing key, instead of replacing the old one with new one
     * It add the new value to the old value.
     * This is useful to count word
     *
     * @param key
     * @param value
     * @return
     */
    @Override
    public Integer put(String key, Integer value) {

        if (this.containsKey(key)) {//if key exists

            int oldVal = this.get(key);

            value = value + oldVal;
            super.put(key, value);

        } else {
            super.put(key, value);
        }
        return value;
    }

}

