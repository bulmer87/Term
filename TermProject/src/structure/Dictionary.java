package structure;

import java.util.Hashtable;

/**
 * Created by Qiu on 4/24/2015.
 * Data structure that stores all words occurs in a document associated with its TF-IDF value.
 * Key is the word, Value is its TF-IDF value
 */

public class Dictionary extends Hashtable<String, Double> {

    /**
     * @param key Word
     * @return  TF-IDF value for a given word, 0 if the word does not exists.
     */
    @Override
    public synchronized Double get(Object key) {
        if (!this.containsKey(key)) {
            return 0.0;
        }
        return super.get(key);
    }
}
