package authordetect.structure;

/**
 * Created by Qiu on 5/4/15.
 */
public class WordTFIDF implements Comparable<WordTFIDF> {

    private String word;
    private Double tfidf;

    public WordTFIDF(String word, double tfidf) {
        this.word = word;
        this.tfidf = tfidf;
    }

    public String getWord() {
        return word;
    }

    public Double getTfidf() {
        return tfidf;
    }

    @Override
    public int compareTo(WordTFIDF o) {
        return tfidf.compareTo(o.getTfidf());
    }
}
