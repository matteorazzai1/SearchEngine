package it.unipi.mircv;

import it.unipi.mircv.baseStructure.*;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

import static it.unipi.mircv.Constants.*;
import static it.unipi.mircv.Utils.*;
import static it.unipi.mircv.baseStructure.SkippingBlock.readSkippingBlocks;

public class Ranking {

    //index is fine as a list, since we do not need to access a specific entry, while the lexicon entry is needed as
    //Map because we need the IDF of a specific term
    /*public static PriorityQueue<Map.Entry<Integer, Double>> DAATDisjunctive(LinkedList<LexiconEntry> lexiconEntries, String query, boolean isBM25, int k) throws IOException {
        HashMap<String, Integer> processedQuery = queryToDict(query);

        PriorityQueue<Map.Entry<Integer, Double>> finalScores = new PriorityQueue<>(k, Map.Entry.comparingByValue());

        Map<String, AbstractMap.SimpleEntry<Integer, Integer>> positions = lexiconEntries.stream()
                .collect(Collectors.toMap(LexiconEntry::getTerm,  LexiconEntry -> new AbstractMap.SimpleEntry<>(0, 1))); //couple which indicates position in the block and numBlock

        DocumentIndex docIndex = DocumentIndex.getInstance();
        docIndex.readFromFile();
        lexiconEntries.sort(Comparator.comparing(LexiconEntry::getDf));
        LinkedList<PostingList> index = new LinkedList<>();

        lexiconEntries.parallelStream().forEach(l -> {try {
            PostingList p = new PostingList(l.getTerm(), readSkippingBlocks(l.getDescriptorOffset()).retrieveBlock());
            index.add(p);
        }catch (IOException e) {
            e.printStackTrace();
        }});

        int nextDocId = minDocID(index, positions);
        double scoreAccumulator;
        Posting currentDoc;

        while(nextDocId != Integer.MAX_VALUE){
            scoreAccumulator = 0;
            for (PostingList p : index){
                if ( p.getPostings().size() > positions.get(p.getTerm())){
                    currentDoc = p.getPostings().get(positions.get(p.getTerm()));
                    if (currentDoc.getDocId() == nextDocId){
                        scoreAccumulator += scoringFunction(isBM25, processedQuery.get(p.getTerm()), currentDoc.getFrequency(),
                                lexiconMap.get(p.getTerm()), docIndex.getDocsLen()[nextDocId-1]);

                        positions.put(p.getTerm(), positions.get(p.getTerm()) + 1);
                    }
                }
            }
            finalScores.add(new AbstractMap.SimpleEntry<>(nextDocId, scoreAccumulator));
            nextDocId = minDocID(index, positions);
        }
        return finalScores;
    }*/

    public static LinkedList DAATConjunctive(LinkedList<LexiconEntry> lexiconEntries, String query, boolean isBM25, int k) throws IOException {
        HashMap<String, Integer> processedQuery = queryToDict(query);
        PriorityQueue<Map.Entry<Integer, Double>> finalScores = new PriorityQueue<>(k, Map.Entry.comparingByValue());

        Map<String, AbstractMap.SimpleEntry<Integer, Integer>> positions = processedQuery.keySet().stream()
                .collect(Collectors.toMap(key -> key, key -> new AbstractMap.SimpleEntry<>(0, 0))); //couple which indicates position in the block and numBlock

        DocumentIndex docIndex = DocumentIndex.getInstance();
        lexiconEntries.sort(Comparator.comparing(LexiconEntry::getDf));
        LinkedList<PostingList> index = new LinkedList<>();

        FileChannel blocks=(FileChannel) Files.newByteChannel(Paths.get(BLOCK_PATH), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        lexiconEntries.parallelStream().forEach(l -> {try {
            PostingList p = new PostingList(l.getTerm(), readSkippingBlocks(l.getDescriptorOffset(), blocks).retrieveBlock());
            synchronized (index) {
                index.add(p);
            }
        }catch (IOException e) {
            e.printStackTrace();
        }});

        index.sort(Comparator.comparing(PostingList::getPostingsLength));
        PostingList shortestPosting = index.get(0);
        int nextDocId = shortestPosting.getPostings().get(0).getDocId();
        double scoreAccumulator;
        boolean computeScore = true;
        AbstractMap.SimpleEntry<Integer, Integer> positionBlockHolder;
        PostingList p;

        while(positions.get(shortestPosting.getTerm()).getValue() < lexiconEntries.get(0).getNumBlocks()){
            for(int j=1; j<index.size(); j++){
                p = index.get(j);
                positionBlockHolder = nextGEQ(p, nextDocId, positions.get(p.getTerm()).getKey(),
                        positions.get(p.getTerm()).getValue(), lexiconEntries.get(j).getDescriptorOffset(), lexiconEntries.get(j).getNumBlocks(), blocks);
                if(positionBlockHolder.getKey() == -1){
                    computeScore = false;
                    if(positionBlockHolder.getValue() != -1){
                        positions.put(p.getTerm(), new AbstractMap.SimpleEntry<>(0, positionBlockHolder.getValue()));
                        break;
                    }
                    else{
                        positions.put(p.getTerm(), new AbstractMap.SimpleEntry<>(0, lexiconEntries.get(0).getNumBlocks()+1));
                        break;
                    }
                }
                else{
                    positions.put(p.getTerm(), positionBlockHolder);
                }
            }

            if(positions.get(shortestPosting.getTerm()).getKey() == shortestPosting.getPostingsLength()-1){
                positions.put(shortestPosting.getTerm(), new AbstractMap.SimpleEntry<>(0, positions.get(shortestPosting.getTerm()).getValue()+1));
                shortestPosting = new PostingList(shortestPosting.getTerm(), readSkippingBlocks(lexiconEntries.get(0).getDescriptorOffset() + (long) positions.get(shortestPosting.getTerm()).getValue()
                        *SkippingBlock.getEntrySize(), blocks).retrieveBlock()) ;
                nextDocId = shortestPosting.getPostings().get(0).getDocId();
            }
            else{
                positions.put(shortestPosting.getTerm(), new AbstractMap.SimpleEntry<>(positions.get(shortestPosting.getTerm()).getKey()+1, positions.get(shortestPosting.getTerm()).getValue()));
                nextDocId = shortestPosting.getPostings().get(positions.get(shortestPosting.getTerm()).getKey()).getDocId();
            }

            if(computeScore){
                scoreAccumulator = 0;
                for (int l=0; l<index.size(); l++){
                    p = index.get(l);
                    scoreAccumulator += scoringFunction(isBM25, processedQuery.get(p.getTerm()), p.getPostings().get(positions.get(p.getTerm()).getKey()).getFrequency(),
                            lexiconEntries.get(l).getIdf(), docIndex.getDocsLen()[nextDocId-1]);
                }

                if(finalScores.size() < k ){
                    finalScores.add(new AbstractMap.SimpleEntry<>(nextDocId-1, scoreAccumulator));
                }
                else if(finalScores.peek().getValue() < scoreAccumulator){
                    finalScores.poll();
                    finalScores.add(new AbstractMap.SimpleEntry<>(nextDocId-1, scoreAccumulator));

                }
            }
        }

        LinkedList<Map.Entry<Integer, Double>> output = new LinkedList<>(finalScores);
        output.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));
        blocks.close();
        return output;
    }

    private static AbstractMap.SimpleEntry<Integer, Integer> nextGEQ(PostingList p, int nextDocId, int position, int block, long descriptorOffset, int numBlocks, FileChannel blockChannel) throws IOException {
        int i,
            j,
            currentId,
            nextBlockVal = p.getPostings().get(p.getPostingsLength()-1).getDocId(); //nextBlockVal is the max docId of the current block

        SkippingBlock skippingBlock = new SkippingBlock();


        if (position+1 == p.getPostingsLength() && block+1 == numBlocks){
            return new AbstractMap.SimpleEntry<>(-1, -1);
        }
        for (i = block; i<numBlocks; i++){
            if (nextBlockVal<nextDocId){
                skippingBlock = readSkippingBlocks(descriptorOffset + (long) i *SkippingBlock.getEntrySize(), blockChannel);
                nextBlockVal = skippingBlock.getMaxDocId();
            }
            else if (i != block){
                p.rewritePostings(skippingBlock.retrieveBlock());
                for(j = 0; j <p.getPostingsLength(); j++){
                    currentId = p.getPostings().get(j).getDocId();
                    if(currentId==nextDocId){
                        return new AbstractMap.SimpleEntry<>(j, i);
                    } else if (currentId>nextDocId) {
                        return new AbstractMap.SimpleEntry<>(-1, i);
                    }
                }
            }

            else{
                for(j = position; j <p.getPostingsLength(); j++){
                    currentId = p.getPostings().get(j).getDocId();
                    if(currentId==nextDocId || currentId>nextDocId){
                        break;
                    }
                }
                return new AbstractMap.SimpleEntry<>(j, i);
            }
        }
        return new AbstractMap.SimpleEntry<>(-1, -1);
    }


    public static void main(String[] args) throws IOException {
        LinkedList<LexiconEntry> entries = new LinkedList();
        String query = "ferrari lamborghini";

        DocumentIndex docIndex = DocumentIndex.getInstance();
        docIndex.loadCollectionStats();
        docIndex.readFromFile();

        long start = System.currentTimeMillis();
        LexiconEntry l1 = Lexicon.retrieveEntryFromDisk("ferrari");
        LexiconEntry l2 = Lexicon.retrieveEntryFromDisk("lamborghini");

        entries.add(l1);
        entries.add(l2);


        System.out.println(DAATConjunctive(entries, query, true, 5));
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: " + timeElapsed);
    }
}
