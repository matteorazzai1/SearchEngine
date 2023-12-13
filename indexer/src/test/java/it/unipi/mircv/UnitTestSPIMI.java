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
        BufferedReader br = createBuffer(PATH_TO_TEST_COLLECTION, false);
        String line;
        String[] docPIDTokens;
        String[] tokens;
        int docID = 1;
        int block_counter = 1;
        boolean terminationFlag = false;
        int docLenAccumulator = 0;
        HashMap<String, PostingList> invertedIndexTest = new HashMap<>();
        ArrayList<Integer> docsLen = new ArrayList<>();

        while (!terminationFlag) {
            while (Runtime.getRuntime().freeMemory() > Runtime.getRuntime().totalMemory() * 20 / 100) {
                line = (br.readLine());
                if (line == null) {
                    terminationFlag = true;
                    break;
                }
                line = Preprocesser.process(line);
                docPIDTokens = line.split("\t"); //split on \t first to get the docID
                if (docPIDTokens.length == 1 || docPIDTokens[1].isBlank())
                    continue;
                tokens = docPIDTokens[1].split(" ");
                docLenAccumulator += tokens.length;
                for (String token : tokens) {
                    PostingList postingList;
                    if (!invertedIndexTest.containsKey(token)) {
                        postingList = new PostingList(new Posting(docID, 1));
                        invertedIndexTest.put(token, postingList);
                        //System.out.println(postingList);
                    } else {
                        invertedIndexTest.get(token).updatePosting(docID);
                    }
                }
                docsLen.add(tokens.length);
                docID++;
            }
            //System.out.println(invertedIndex);
            invertedIndexTest = invertedIndexTest.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (e1, e2) -> e1, LinkedHashMap::new));
            //System.out.println(invertedIndex);
        }
        br.close();
        return new Object[]{invertedIndexTest, docsLen};
    }


}
