package it.unipi.mircv.baseStructure;

import it.unipi.mircv.compression.UnaryCompressor;
import it.unipi.mircv.compression.VariableByteCompressor;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;

import static it.unipi.mircv.Constants.INV_INDEX_DOCID;
import static it.unipi.mircv.Constants.INV_INDEX_FREQS;

public class PostingList {

    public String term;

    private ArrayList<Posting> postings = new ArrayList<>();

    public PostingList() {
    }

    public PostingList(Posting p) {
        postings.add(p);
    }

    /**
     * This constructor is used to create the posting list from a line of the intermediate index file
     * @param line is the line of the intermediate index file
     */
    public PostingList(String line){

        String[] parts = line.split("\t");

        this.term= parts[0];

        if(parts[1] != null) {
            String[] parts_posting = parts[1].split(" ");  //parts[1] is the complete posting list for a term


            for (int i = 0; i < parts_posting.length; i++) {
                String[] pair = parts_posting[i].split(":");
                if (pair.length == 2) {
                    int x = Integer.parseInt(pair[0]);
                    int y = Integer.parseInt(pair[1].trim());


                    Posting posting = new Posting(x, y);

                    this.postings.add(posting);

                }
            }
        }
    }
    public PostingList(String term, ArrayList<Posting> postings){
            this.term=term;
            this.postings=postings;
    }

    public PostingList(PostingList p){
        this.term = p.term;
        this.postings = new ArrayList<>(p.postings.size());
        this.postings.addAll(p.postings);
    }

    public String getTerm() {
        return term;
    }
    public ArrayList<Posting> getPostings(){
        return postings;
    }

    @Override
    public String toString() {
        StringBuilder postingList=new StringBuilder();

        postingList.append(term+"\t");

        for(Posting post:postings){
                postingList.append(post.getDocId()+":"+post.getFrequency()+" ");
        }
        postingList.deleteCharAt(postingList.length()-1); //to delete the last space
        postingList.append("\n");
        return postingList.toString();
    }

    /**
     * This method is used to merge the posting list of the intermediate index, in order to create the final posting list
     * @param intermediatePostingList is the posting list of the intermediate index for a specific term
     */
    public void appendList(PostingList intermediatePostingList) {
            //here we have to add the posting keeping the sorting in base of the docId
        this.postings.addAll(intermediatePostingList.postings);
        this.postings.sort(Comparator.comparing(Posting::getDocId));
    }

    /**
     * This method updates the frequency of the posting list for a specific document during SPIMI
     * @param docID is the document id of the posting that we want to update in the posting list
     */
    public void updatePosting(int docID){
        if (postings.get(postings.size() - 1).getDocId() == docID)
        {
            Posting p = postings.get(postings.size() - 1);
            p.setFrequency(p.getFrequency() + 1);
            postings.set(postings.size() - 1, p);
        }
        else{
            Posting p = new Posting(docID, 1);
            postings.add(p);
        }

    }

    public void rewritePostings(ArrayList<Posting> newPostings) {
        this.postings = newPostings;
    }

    public int getPostingsLength() {
        return this.getPostings().size();
    }
}
