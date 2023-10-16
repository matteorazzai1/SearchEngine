package it.unipi.mircv;

import java.util.ArrayList;
import java.util.Comparator;

public class PostingList {

    public String term;

    private final ArrayList<Posting> postings=new ArrayList<>();


    public PostingList(String line){

        String[] parts = line.split(" ");

        this.term= parts[0];

        for (int i = 1; i < parts.length; i++) {
            String[] pair = parts[i].split(":");
            if (pair.length == 2) {
                int x = Integer.parseInt(pair[0]);
                int y = Integer.parseInt(pair[1]);


                Posting posting=new Posting(x,y);

                this.postings.add(posting);

            }
        }
    }

    public String getTerm() {
        return term;
    }

    @Override
    public String toString() {
        StringBuilder postingList=new StringBuilder();

        postingList.append(term);

        for(Posting post:postings){
                postingList.append(" "+post.getDocId()+":"+post.getFrequency());
        }
        postingList.append("\n");
        return postingList.toString();
    }

    public void appendList(PostingList intermediatePostingList) {
            //here we have to add the posting keeping the sorting in base of the docId
        this.postings.addAll(intermediatePostingList.postings);
        this.postings.sort(Comparator.comparing(Posting::getDocId));
    }
}
