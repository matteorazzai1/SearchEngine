package it.unipi.mircv.baseStructure;

import java.util.HashMap;

public class InvertedIndex {

    private static InvertedIndex instance;
    private HashMap<String, PostingList> PostingLists;

    private InvertedIndex(){
        PostingLists = new HashMap<>();
    }

    public static InvertedIndex getInstance(){
        if (instance == null) {
            instance = new InvertedIndex();
        }
        return instance;
    }

    public static void resetInstance(){
        instance = null;
    }

    public HashMap<String, PostingList> getPostingLists() {
        return PostingLists;
    }

    public void setPostingLists(HashMap<String, PostingList> postingLists) {
        InvertedIndex.getInstance().PostingLists = postingLists;
    }
}
