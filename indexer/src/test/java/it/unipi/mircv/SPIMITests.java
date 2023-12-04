package it.unipi.mircv;

import it.unipi.mircv.baseStructure.Posting;
import it.unipi.mircv.baseStructure.PostingList;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import static org.junit.Assert.assertTrue;

public class SPIMITests {
    @Test
    //test the SPIMI algorithm
    public void testSPIMI() throws IOException {
        HashMap<String, PostingList> expectedIndex = new LinkedHashMap<>();
        ArrayList<Integer> expectedDocsLen = new ArrayList<>();

        Posting bike1 = new Posting(1, 1);
        Posting bike2 = new Posting(2, 1);
        PostingList pl1 = new PostingList("null", new ArrayList<>() {{
            add(bike1);
            add(bike2);
        }});
        Posting chair1 = new Posting(1, 1);
        PostingList pl2 = new PostingList(chair1);
        Posting home1 = new Posting(1, 1);
        PostingList pl3 = new PostingList(home1);
        Posting kiwi1 = new Posting(1, 2);
        PostingList pl4 = new PostingList(kiwi1);
        Posting sun1 = new Posting(2, 1);
        PostingList pl5 = new PostingList(sun1);
        Posting tree1 = new Posting(2, 1);
        PostingList pl6 = new PostingList(tree1);
        Posting water1 = new Posting(1, 1);
        PostingList pl7 = new PostingList(water1);
        expectedIndex.put("bike", pl1);
        expectedIndex.put("chair", pl2);
        expectedIndex.put("home", pl3);
        expectedIndex.put("kiwi", pl4);
        expectedIndex.put("sun", pl5);
        expectedIndex.put("tree", pl6);
        expectedIndex.put("water", pl7);

        expectedDocsLen.add(6);
        expectedDocsLen.add(3);

        Object[] objects=UnitTestSPIMI.performUnitTestSPIMI();
        HashMap<String, PostingList> actualIndex = (HashMap<String, PostingList>) objects[0];
        ArrayList<Integer> actualDocsLen = (ArrayList<Integer>) objects[1];
        //System.out.println(actualIndex);
        //System.out.println(expectedIndex);
        assertTrue(actualIndex.toString().equals(expectedIndex.toString()));
        assertTrue(actualDocsLen.toString().equals(expectedDocsLen.toString()));
    }

}
