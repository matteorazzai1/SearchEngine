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

        Map<String, AbstractMap.SimpleEntry<Integer, Integer>> positions = lexiconEntries.stream()
                .collect(Collectors.toMap(LexiconEntry::getTerm,  LexiconEntry -> new AbstractMap.SimpleEntry<>(0, 1))); //couple which indicates position in the block and numBlock

        DocumentIndex docIndex = DocumentIndex.getInstance();
        lexiconEntries.sort(Comparator.comparing(LexiconEntry::getDf));
        LinkedList<PostingList> index = new LinkedList<>();

        FileChannel blocks=(FileChannel) Files.newByteChannel(Paths.get(BLOCK_PATH), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        lexiconEntries.parallelStream().forEach(l -> {try {
            PostingList p = new PostingList(l.getTerm(), readSkippingBlocks(l.getDescriptorOffset(), blocks).retrieveBlock());
            index.add(p);
        }catch (IOException e) {
            e.printStackTrace();
        }});

        index.sort(Comparator.comparing(PostingList::getPostingsLength));
        PostingList shortestPosting = index.get(0);
        int nextDocId = shortestPosting.getPostings().get(0).getDocId();
        double scoreAccumulator;
        boolean terminationFlag = false;
        boolean computeScore;
        AbstractMap.SimpleEntry<Integer, Integer> positionBlockHolder;

        while(!terminationFlag){
            computeScore = true;
            for(int j=1; j<index.size(); j++){
                PostingList p = index.get(j);
                positionBlockHolder = nextGEQ(p, nextDocId, positions.get(p.getTerm()).getKey(),
                        positions.get(p.getTerm()).getValue(), lexiconEntries.get(j).getDescriptorOffset(), lexiconEntries.get(j).getNumBlocks(), blocks);
                if(positionBlockHolder.getKey() == -1){
                    computeScore = false;
                    positionBlockHolder = nextGEQ(shortestPosting, nextDocId+1, positions.get(shortestPosting.getTerm()).getKey(),
                            positions.get(shortestPosting.getTerm()).getValue(), lexiconEntries.get(0).getDescriptorOffset(), lexiconEntries.get(0).getNumBlocks(), blocks);
                    if(positionBlockHolder.getKey() == -1){
                        terminationFlag = true;
                        break;
                    }
                    else{
                        positions.put(shortestPosting.getTerm(), positionBlockHolder);
                        break;
                    }
                }
                else{
                    positions.put(p.getTerm(), positionBlockHolder);
                    index.set(j, p);
                }
            }
            if(computeScore){
                scoreAccumulator = 0;
                for (int l=0; l<index.size(); l++){
                    PostingList p = index.get(l);
                    scoreAccumulator += scoringFunction(isBM25, processedQuery.get(p.getTerm()), p.getPostings().get(positions.get(p.getTerm()).getKey()).getFrequency(),
                            lexiconEntries.get(l).getIdf(), docIndex.getDocsLen()[nextDocId-1]);
                }

                if(finalScores.size() == k && finalScores.peek().getValue() < scoreAccumulator){
                    finalScores.poll();
                    finalScores.add(new AbstractMap.SimpleEntry<>(nextDocId-1, scoreAccumulator));
                }
            }
            nextDocId = shortestPosting.getPostings().get(positions.get(shortestPosting.getTerm()).getKey()).getDocId();
        }

        LinkedList<Map.Entry<Integer, Double>> output = new LinkedList<>(finalScores);
        output.sort(Map.Entry.comparingByValue());
        blocks.close();
        return output;
    }

    private static AbstractMap.SimpleEntry<Integer, Integer> nextGEQ(PostingList p, int nextDocId, int position, int block, long descriptorOffset, int numBlocks, FileChannel blockChannel) throws IOException {
        boolean toReturn = false;
        int i, j, blockSize = 0, positionAcc = 0, nextBlockVal = p.getPostings().get(-1).getDocId();
        SkippingBlock skippingBlock = new SkippingBlock();
        if (position+1 == p.getPostingsLength()){
            return new AbstractMap.SimpleEntry<>(-1, -1);
        }
        for (i = block; i<numBlocks; i++){
            if (nextBlockVal<nextDocId){
                readSkippingBlocks(descriptorOffset + (long) i *SkippingBlock.getEntrySize(), blockChannel);
                nextBlockVal = skippingBlock.getMaxDocId();
            }
            else{
                toReturn = true;
                blockSize = skippingBlock.getNumPostings();
                break;
            }
        }
        if (!toReturn) {
            return new AbstractMap.SimpleEntry<>(-1, skippingBlock.getNumPostings()-1);//-1 perche' potrei avere ancora valori da considerare nel blocco
        }
        else{
            for(j = positionAcc; j <positionAcc+blockSize; j++){
                if(p.getPostings().get(j).getDocId()==nextDocId){
                    break;
                }
            }
            return new AbstractMap.SimpleEntry<>(j, i);
        }

    }


    public static void main(String[] args) throws IOException {
        LinkedList<LexiconEntry> entries = new LinkedList();
        String query = "ferrari lamborghini";

        LexiconEntry l1 = Lexicon.retrieveEntryFromDisk("ferrari");
        LexiconEntry l2 = Lexicon.retrieveEntryFromDisk("lamborghini");

        entries.add(l1);
        entries.add(l2);

        System.out.println(DAATConjunctive(entries, query, false, 10));
    }
}
