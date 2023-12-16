package it.unipi.mircv;

import it.unipi.mircv.baseStructure.InvertedIndex;
import it.unipi.mircv.baseStructure.Posting;
import it.unipi.mircv.baseStructure.PostingList;
import static it.unipi.mircv.Constants.*;
import static it.unipi.mircv.FileUtils.createBuffer;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class UnitTestSPIMI {

    public static final String PATH_TO_TEST_COLLECTION = "src/test/data/collectionTest.tsv";
    //a SPIMI algorithm implementation for unit testing with some modifications
    public static Object[] performUnitTestSPIMI() throws IOException{
        BufferedReader br = createBuffer(PATH_TO_TEST_COLLECTION, false); //create the buffered reader
        // variables for the SPIMI algorithm
        String line;
        String[] docPIDTokens;
        String[] tokens;
        int docID = 1; //initialize the docID
        boolean terminationFlag = false; //flag for the termination of the algorithm
        int docLenAccumulator = 0; //accumulator for the document length
        HashMap<String, PostingList> invertedIndexTest = new HashMap<>(); //an HashMap for the inverted index
        ArrayList<Integer> docsLen = new ArrayList<>(); //an ArrayList for the documents length
        //SPIMI algorithm
        while (!terminationFlag) { //while the termination flag is false
            //while the memory is not full
            while (Runtime.getRuntime().freeMemory() > Runtime.getRuntime().totalMemory() * 20 / 100) {
                line = (br.readLine()); //read a line from the collection
                if (line == null) { //if the line is null
                    terminationFlag = true; //set the termination flag to true
                    break; //break the while loop
                }
                line = Preprocesser.process(line); //process the line
                docPIDTokens = line.split("\t"); //split on \t first to get the docID
                if (docPIDTokens.length == 1 || docPIDTokens[1].isBlank()) //if the line is empty skip it
                    continue;
                tokens = docPIDTokens[1].split(" "); //split on space to get the tokens
                docLenAccumulator += tokens.length; //update the document length accumulator
                for (String token : tokens) { //for each token
                    PostingList postingList; //initialize the posting list
                    if (!invertedIndexTest.containsKey(token)) { //if the token is not in the inverted index
                        postingList = new PostingList(new Posting(docID, 1)); //create a new posting list with the current docID and frequency 1
                        invertedIndexTest.put(token, postingList); //put the posting list in the inverted index with the token as key
                    } else { //if the token is in the inverted index
                        invertedIndexTest.get(token).updatePosting(docID); //update the posting list with the current docID
                    }
                }
                docsLen.add(tokens.length); //add the document length to the ArrayList
                docID++; //update the docID
            }
            //put the inverted index in the HashMap and sort it
            invertedIndexTest = invertedIndexTest.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (e1, e2) -> e1, LinkedHashMap::new));
        }
        br.close(); //close the buffered reader
        return new Object[]{invertedIndexTest, docsLen};
    }


}
