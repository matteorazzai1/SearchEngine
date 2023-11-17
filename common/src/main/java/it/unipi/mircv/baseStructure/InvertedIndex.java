package it.unipi.mircv.baseStructure;

import java.util.HashMap;

public class InvertedIndex {

    private HashMap<String, PostingList> PostingLists;

    public InvertedIndex(){
        PostingLists = new HashMap<>();
    }

    public HashMap<String, PostingList> getPostingLists() {
        return PostingLists;
    }

    public void setPostingLists(HashMap<String, PostingList> postingLists) {
        this.PostingLists = postingLists;
    }
}
