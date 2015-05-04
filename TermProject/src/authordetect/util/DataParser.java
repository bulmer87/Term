package authordetect.util;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;


public class DataParser {
    public HashMap<String, HashMap<String, Double>> authors = new HashMap<String, HashMap<String, Double>>();
    public HashMap<String, HashMap<String, Double>> books = new HashMap<String, HashMap<String, Double>>();
    public HashMap<String, HashMap<String, Double>> euclideanMat = new HashMap<String, HashMap<String, Double>>();
    public HashMap<String, HashMap<String, Double>> cosineMat = new HashMap<String, HashMap<String, Double>>();

    public ArrayList<String> findPositives = new ArrayList<String>();
    double time = 0;
    int truePos = 0;
    int falsePos = 0;
    int falseNeg = 0;

    public void readin(String infileOne, String infileTwo) {
        File trainingFile = new File(infileOne);

        try {
            FileInputStream instream = new FileInputStream(trainingFile);
            DataInputStream in = new DataInputStream(instream);
            BufferedReader buf = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while ((line = buf.readLine()) != null) {
                if (line.isEmpty() || line.trim().length() == 0) {
                    continue;
                }
                HashMap<String, Double> map = new HashMap<String, Double>();
                String[] sp = line.split("\\t");
                String[] sp2 = sp[1].split(",");
                for (int i = 0; i < sp2.length; i++) {
                    String[] s = sp2[i].split("=");
                    map.put(s[0].trim(), Double.parseDouble(s[1].trim()));
                }
                authors.put(sp[0].trim(), map);
            }
            buf.close();
//            } catch (Exception e) {
//                System.err.println("!!!!! " + e + " !!!!!!!!!");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File testingFile = new File(infileTwo);
        try {
            FileInputStream instream = new FileInputStream(testingFile);
            DataInputStream in = new DataInputStream(instream);
            BufferedReader buf = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while ((line = buf.readLine()) != null) {
                if (line.isEmpty() || line.trim().length() == 0) {
                    continue;
                }
                HashMap<String, Double> map = new HashMap<String, Double>();
                String[] sp = line.split("\\t");
                String[] sp2 = sp[1].split(",");
                for (int i = 0; i < sp2.length; i++) {
                    String[] s = sp2[i].split("=");
                    map.put(s[0].trim(), Double.parseDouble(s[1].trim()));
                }
                books.put(sp[0].trim(), map);
            }
            buf.close();
        } catch (Exception e) {
            System.err.println("!!!!! " + e + " !!!!!!!!!");
        }

    }

    public double computeDistanceMatrix() {
        final double startTime = System.currentTimeMillis();
        double largestDistance = -1000000.0;

        for (String book : books.keySet()) // iterate through all the books
        {
            HashMap<String, Double> bookToAuthor = new HashMap<String, Double>();  // create a mapping of authors and their distance to that book
            String[] authorSplit = book.split("_");//split to get the author appended to book
            for (String author : authors.keySet()) // for each book iterate through all the possible authors
            {
                if (authorSplit[1].equals(author))//if the author appended to book matches the training set author add them to the array list to confirm positives
                {
                    findPositives.add(authorSplit[1]);//this will only add the author if the true author appended to book matches the author that occurs in the training set
                }
                double similarity = 0.0;
                HashMap<String, Double> bookMap = new HashMap<String, Double>();// hold the hashmap of TFIDF values that the specific book has for each letter
                bookMap.putAll(books.get(book)); //make a copy
                HashMap<String, Double> authorMap = authors.get(author);// hold the hashmap of TFIDF values that the specific author has for each letter
                for (String letter : authorMap.keySet())// iterate through the authors TFIDF letters and see if they are contained within the Books TFIDF letters
                {
                    if (bookMap.containsKey(letter))// if both maps contain the same letter compute the distance between the two
                    {
                        similarity += Math.pow((authorMap.get(letter) - bookMap.get(letter)), 2);
                        bookMap.remove(letter);// remove that letter from the bookMap
                        //System.out.println("Success for Book " + book + " and " + author + " the word // " + letter + " // match!\n Here is similarity so far : " + similarity + "\n\n");
                    } else// if only the author contains that letter compute the similarity
                    {
                        similarity += Math.pow(authorMap.get(letter), 2);
                        //System.out.println("Un-Successful match for Book " + book + " and " + author + " the word // " + letter + " // dont match!\n Here is similarity so far : " + similarity + "\n\n");
                    }
                }
                if (bookMap.isEmpty() == false)// if there are any remaining letters left in that book get all the values and compute distance
                {
                    for (String letterLeft : bookMap.keySet()) {
                        similarity += Math.pow(bookMap.get(letterLeft), 2);
                        //System.out.println("Here are the letters left // " + letterLeft + " // and similarity " + similarity + "\n\n");
                    }
                }
                similarity = Math.sqrt(similarity);//compute the final similarity
                if (similarity > largestDistance) // check to see if that value is the largest value yet to normalize the distances
                {
                    largestDistance = similarity;
                }
                bookToAuthor.put(author, similarity); // put that authors name and similarity into the hashmap for that book

            }
            euclideanMat.put(book, bookToAuthor);// for that book put in the hashmap that contains the name and similarities for all authors.
        }
        final double endTime = System.currentTimeMillis();
        time = (endTime - startTime) / 1000;
        return largestDistance; // this will be returned and fed into normalize
    }

    public void computeCosineDistanceMatrix() {
        final double startTime = System.currentTimeMillis();

        for (String book : books.keySet()) // iterate through all the books
        {
            HashMap<String, Double> bookToAuthor = new HashMap<String, Double>();  // create a mapping of authors and their distance to that book
            String[] authorSplit = book.split("_");//split to get the author appended to book
            for (String author : authors.keySet()) // for each book iterate through all the possible authors
            {
                if (authorSplit[1].equals(author))//if the author appended to book matches the training set author add them to the array list to confirm positives
                {
                    findPositives.add(authorSplit[1]);//this will only add the author if the true author appended to book matches the author that occurs in the training set
                }
                double similarity = 0.0;
                double dotProduct = 0.0;
                double magAuthor = 0.0;
                double magBook = 0.0;
                HashMap<String, Double> bookMap = books.get(book);// hold the hashmap of TFIDF values that the specific book has for each letter
                HashMap<String, Double> authorMap = authors.get(author);// hold the hashmap of TFIDF values that the specific author has for each letter
                for (String letter : authorMap.keySet())// iterate through the authors TFIDF letters and see if they are contained within the Books TFIDF letters
                {
                    if (bookMap.containsKey(letter))// if both maps contain the same letter compute the distance between the two
                    {
                        dotProduct += authorMap.get(letter) * bookMap.get(letter);
                        //System.out.println("Success for Book " + book + " and " + author + " the word // " + letter + " // match!\n Here is similarity so far : " + similarity + "\n\n");
                    }
                }
                for (Double tfidf : authorMap.values()) {
                    magAuthor += Math.pow(tfidf, 2);
                }
                magAuthor = Math.sqrt(magAuthor);
                for (Double tfidf : bookMap.values()) {
                    magBook += Math.pow(tfidf, 2);
                }
                magBook = Math.sqrt(magBook);
                similarity = dotProduct / (magAuthor * magBook);
//                if (similarity > largestDistance) // check to see if that value is the largest value yet to normalize the distances
//                {
//                    largestDistance = similarity;
//                }
                bookToAuthor.put(author, similarity); // put that authors name and similarity into the hashmap for that book

            }
            cosineMat.put(book, bookToAuthor);// for that book put in the hashmap that contains the name and similarities for all authors.
        }
        final double endTime = System.currentTimeMillis();
        time = (endTime - startTime) / 1000;
    }

    public void normalize(double normalizedNum)//normalize the values to get a bound from 0 to 1 to set a threshold that is accurate from either 0 to 1;
    {
        for (String bookNames : euclideanMat.keySet())// go through all the books that have been compared to authors and have a distance between them
        {
            HashMap<String, Double> retMap = euclideanMat.get(bookNames);//get the hashmap at that book title. This map holds the name of the authors and their distance between the book
            for (String retAuthor : retMap.keySet())// get all the distances between authors that pertain to that book and normalize them based off the highest distance found.
            {
                double normalized = retMap.get(retAuthor) / normalizedNum;
                retMap.put(retAuthor, normalized); // replace that authors distance value with a normalized distance value
            }
            euclideanMat.put(bookNames, retMap); // replace that book with a hashmap of normalized values for all the authors
        }
    }

    public void outputResults(String infile, double threshold) throws FileNotFoundException, UnsupportedEncodingException {
        try {
            PrintWriter outWriter = new PrintWriter(infile, "UTF8");

            for (String book : euclideanMat.keySet()) {
                String[] split = book.split("_");
                String guessedAuthor = "NULL";
                double smallestValue = 10000000;
                HashMap<String, Double> temp = euclideanMat.get(book);
                for (String author : temp.keySet()) {
                    if (temp.get(author) < smallestValue && temp.get(author) <= threshold) {
                        guessedAuthor = author;
                        smallestValue = temp.get(author);
                    }
                    System.out.println("Book: " + split[0] + " smallest value: " + smallestValue + " guessedAuthor " + guessedAuthor);
                }
                computePN(guessedAuthor, split[1], split[0], outWriter);
            }
            double F1 = computeF1Score();
            outWriter.println("The F1 score for this run is: " + F1);
            outWriter.println("The time took for matrix multiplication: " + time + "\n");
            outWriter.close();
        } catch (Exception e) {
            System.out.println("!!!!!! " + e + " !!!!!!!!\n");
        }

    }

    public void outputResultsCosine(String infile, double threshold) throws FileNotFoundException, UnsupportedEncodingException {
        try {
            PrintWriter outWriter = new PrintWriter(infile, "UTF8");

            for (String book : cosineMat.keySet()) {
                String[] split = book.split("_");
                String guessedAuthor = "NULL";
                double largestValue = -1;
                HashMap<String, Double> temp = cosineMat.get(book);
                for (String author : temp.keySet()) {
                    if (temp.get(author) > largestValue && temp.get(author) >= threshold) {
                        guessedAuthor = author;
                        largestValue = temp.get(author);
                    }
                    System.out.println("Book: " + split[0] + " smallest value: " + largestValue + " guessedAuthor " + guessedAuthor);
                }
                computePN(guessedAuthor, split[1], split[0], outWriter);
            }
            double F1 = computeF1Score();
            outWriter.println("The F1 score for this run is: " + F1);
            outWriter.println("The time took for matrix multiplication: " + time + "\n");
            outWriter.close();
        } catch (Exception e) {
            System.out.println("!!!!!! " + e + " !!!!!!!!\n");
        }

    }

    public void computePN(String guessedAuthor, String trueAuthor, String book, PrintWriter outWriter) {
        if (guessedAuthor.equals(trueAuthor) && findPositives.contains(trueAuthor)) {
            truePos++;
            outWriter.println("//TRUE POSITIVE// the book: " + book + " is written by: " + guessedAuthor + " and this author is contained in training set");
        } else if (guessedAuthor.equals(trueAuthor) == false && findPositives.contains(trueAuthor) && guessedAuthor.equals("NULL") == false) {
            falsePos++;
            outWriter.println("//FALSE POSITIVE// the book: " + book + " was predicted to have author: " + guessedAuthor + " but was written by: " + trueAuthor + " and this author is contained in training set");
        } else if (guessedAuthor.equals("NULL") == false && findPositives.contains(trueAuthor) == false) {
            falsePos++;
            outWriter.println("//FALSE POSITIVE// the book: " + book + " was predicted to have author: " + guessedAuthor + " but the true author was not in training set");
        } else if (guessedAuthor.equals("NULL") && findPositives.contains(trueAuthor)) {
            falseNeg++;
            outWriter.println("//FALSE NEGATIVE// the book " + book + " was predicted to not have author in training set but author is contained in training set and is: " + trueAuthor);
        } else {
            outWriter.println("//TRUE NEGATIVE// the book " + book + " did not have an author in our training set.");
        }
    }

    public double computeF1Score() {
        System.out.println("TruePos: " + truePos);
        System.out.println("FalsePos: " + falsePos);
        System.out.println("FalseNeg: " + falseNeg);
        double top = 2 * truePos;
        double bottom = (2 * truePos) + falsePos + falseNeg;
        double result = top / bottom;
        return result;
    }

    public void outputMaps(HashMap<String, HashMap<String, Double>> inMap) {
        for (String keyString : inMap.keySet()) {
            HashMap<String, Double> output = inMap.get(keyString);
            System.out.print(keyString + "\t values: \t");
            for (String letter : output.keySet()) {
                System.out.print(letter + ":" + output.get(letter) + "\t");
            }
            System.out.println("\n\n");
        }
    }

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        for (int i = 0; i < 3; i++) {
            DataParser myParser = new DataParser();
            String trainingPath = args[0] + "/" + i + "/part-r-00000";
            String testPath = args[1] + "/" + i + "/part-r-00000";
            myParser.readin(trainingPath, testPath);
            //myParser.outputMaps(myParser.authors);
            //myParser.outputMaps(myParser.books);
            double normValue = myParser.computeDistanceMatrix();
            //System.out.println("This is the largest distance found : " + normValue + "\n\n");
            //System.out.println("This is time took: " + myParser.time + "\n\n");
            myParser.computeCosineDistanceMatrix();
            myParser.outputResultsCosine(args[4] + i, Double.parseDouble(args[5]));
            myParser.normalize(normValue);
            //   myParser.outputMaps(myParser.euclideanMat);
            myParser.outputResults(args[2] + i, Double.parseDouble(args[3]));
        }

    }
}
