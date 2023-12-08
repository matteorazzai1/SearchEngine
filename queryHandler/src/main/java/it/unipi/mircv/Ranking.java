package it.unipi.mircv;

import ca.rmen.porterstemmer.PorterStemmer;
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
    public static LinkedList<Map.Entry<Integer, Double>> DAATDisjunctive(String query, boolean isBM25, int k) throws IOException {
        HashMap<String, Integer> processedQuery = queryToDict(query);
        PriorityQueue<Map.Entry<Integer, Double>> finalScores = new PriorityQueue<>(k, Map.Entry.comparingByValue());

        ArrayList<LexiconEntry> lexiconEntries = new ArrayList<>();

        for(Map.Entry<String, Integer> e:processedQuery.entrySet()){
            //System.out.println(e);
            String term=e.getKey();
            LexiconEntry entry = Lexicon.retrieveEntryFromDisk(term);
            lexiconEntries.add(entry);
        }


        Map<String, AbstractMap.SimpleEntry<Integer, Integer>> positions = processedQuery.keySet().stream()
                .collect(Collectors.toMap(key -> key, key -> new AbstractMap.SimpleEntry<>(0, 0))); //couple which indicates position in the block and numBlock

        DocumentIndex docIndex = DocumentIndex.getInstance();
        ArrayList<PostingList> index = new ArrayList<>();

        FileChannel blocks=(FileChannel) Files.newByteChannel(Paths.get(BLOCK_PATH), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        lexiconEntries.parallelStream().forEach(l -> {try {
            PostingList p = new PostingList(l.getTerm(), readSkippingBlocks(l.getDescriptorOffset(), blocks).retrieveBlock());
            synchronized (index) {
                index.add(p);
            }
        }catch (IOException e) {
            e.printStackTrace();
        }});

        int nextDocId = minDocID(index, positions);
        double scoreAccumulator;
        AbstractMap.SimpleEntry<Integer, Integer> positionBlockHolder;
        PostingList p;
        long descriptorOffset;
        int numBlocks;

        lexiconEntries.sort(Comparator.comparing(LexiconEntry::getTerm));
        index.sort(Comparator.comparing(PostingList::getTerm));

        while(nextDocId != Integer.MAX_VALUE){
            scoreAccumulator = 0;
            for (int j = 0; j < index.size(); ) {

                p = index.get(j);
                Map.Entry<Integer, Integer> termPositions = positions.get(p.getTerm());
                descriptorOffset = lexiconEntries.get(j).getDescriptorOffset();
                numBlocks = lexiconEntries.get(j).getNumBlocks();

                positionBlockHolder = nextGEQ(p, nextDocId, termPositions.getKey(),
                termPositions.getValue(), descriptorOffset, numBlocks, blocks);


                if (positionBlockHolder == null) {
                    index.remove(j);
                    lexiconEntries.remove(j);
                    continue;
                }

                Posting postingEntry = p.getPostings().get(positionBlockHolder.getKey());
                if (postingEntry.getDocId() == nextDocId) {
                    scoreAccumulator += scoringFunction(
                            isBM25,
                            processedQuery.get(p.getTerm()),
                            postingEntry.getFrequency(),
                            lexiconEntries.get(j).getIdf(),
                            docIndex.getDocsLen()[nextDocId - 1]
                    );

                    //fastest way to move forward by one, since I may need to go in the next block, so I use nextDocId+1
                    positionBlockHolder = nextGEQ(p, nextDocId + 1, termPositions.getKey(),
                            termPositions.getValue(), descriptorOffset, numBlocks, blocks);


                    if (positionBlockHolder == null) {
                        index.remove(j);
                        lexiconEntries.remove(j);
                        continue;
                    }

                }
                positions.put(p.getTerm(), new AbstractMap.SimpleEntry<>(positionBlockHolder.getKey(), positionBlockHolder.getValue()));
                //we update here since we may have removed an element
                j++;
            }

            index.trimToSize();
            lexiconEntries.trimToSize();

            if(finalScores.size() < k){
                finalScores.add(new AbstractMap.SimpleEntry<>(nextDocId, scoreAccumulator));
            }
            else if(finalScores.peek().getValue() < scoreAccumulator){
                finalScores.poll();
                finalScores.add(new AbstractMap.SimpleEntry<>(nextDocId, scoreAccumulator));

            }
            nextDocId = minDocID(index, positions);

        }
        LinkedList<Map.Entry<Integer, Double>> output = new LinkedList<>(finalScores);
        output.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));
        blocks.close();
        return output;
    }

    public static LinkedList DAATConjunctive(ArrayList<LexiconEntry> lexiconEntries, String query, boolean isBM25, int k) throws IOException {
        HashMap<String, Integer> processedQuery = queryToDict(query);
        PriorityQueue<Map.Entry<Integer, Double>> finalScores = new PriorityQueue<>(k, Map.Entry.comparingByValue());

        Map<String, AbstractMap.SimpleEntry<Integer, Integer>> positions = processedQuery.keySet().stream()
                .collect(Collectors.toMap(key -> key, key -> new AbstractMap.SimpleEntry<>(0, 0))); //couple which indicates positionHolder in the block and numBlock

        DocumentIndex docIndex = DocumentIndex.getInstance();
        ArrayList<PostingList> index = new ArrayList<>();

        FileChannel blocks=(FileChannel) Files.newByteChannel(Paths.get(BLOCK_PATH), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        lexiconEntries.parallelStream().forEach(l -> {try {
            PostingList p = new PostingList(l.getTerm(), readSkippingBlocks(l.getDescriptorOffset(), blocks).retrieveBlock());
            synchronized (index) {
                index.add(p);
            }
        }catch (IOException e) {
            e.printStackTrace();
        }});

        lexiconEntries.sort(Comparator.comparing(LexiconEntry::getDf));
        index.sort(Comparator.comparing(PostingList::getPostingsLength));
        PostingList shortestPosting = index.get(0);
        LexiconEntry shortestLexiconEntry = lexiconEntries.get(0);
        int nextDocId = shortestPosting.getPostings().get(0).getDocId();
        double scoreAccumulator;
        boolean toCompute;
        AbstractMap.SimpleEntry<Integer, Integer> positionBlockHolder;
        PostingList p;



        while(true){
            toCompute = true;
            for(int j=1; j<index.size(); j++){
                p = index.get(j);
                Map.Entry<Integer, Integer> termPositions = positions.get(p.getTerm());

                positionBlockHolder = nextGEQ(p, nextDocId, termPositions.getKey(),
                        termPositions.getValue(), lexiconEntries.get(j).getDescriptorOffset(), lexiconEntries.get(j).getNumBlocks(), blocks);

                //one of the postings is finished, so we can stop. This may happen having postings like (1000:1, 1001:1), (10:1, 20:1, 30:1)
                if(positionBlockHolder == null){
                    LinkedList<Map.Entry<Integer, Double>> output = new LinkedList<>(finalScores);
                    output.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));
                    blocks.close();
                    return output;
                }

                positions.put(p.getTerm(), new AbstractMap.SimpleEntry<>(positionBlockHolder.getKey(), positionBlockHolder.getValue()));

                if (p.getPostings().get(positionBlockHolder.getKey()).getDocId() != nextDocId){
                    toCompute = false;
                    break;
                }
        }

            if(toCompute){
                scoreAccumulator = 0;
                for (int l=0; l<index.size(); l++){
                    p = index.get(l);
                    scoreAccumulator += scoringFunction(isBM25, processedQuery.get(p.getTerm()), p.getPostings().get(positions.get(p.getTerm()).getKey()).getFrequency(),
                            lexiconEntries.get(l).getIdf(), docIndex.getDocsLen()[nextDocId-1]);
                }

                if(finalScores.size() < k ){
                    finalScores.add(new AbstractMap.SimpleEntry<>(nextDocId, scoreAccumulator));
                }
                else if(finalScores.peek().getValue() < scoreAccumulator){
                    finalScores.poll();
                    finalScores.add(new AbstractMap.SimpleEntry<>(nextDocId, scoreAccumulator));
                }
            }

            //i have to go on the next position of the shortest posting list, so i use nextDocID+1 and call nextGEQ, if we get null we're at the end of the posting list
            positionBlockHolder = nextGEQ(shortestPosting, nextDocId + 1, positions.get(shortestPosting.getTerm()).getKey(),
                    positions.get(shortestPosting.getTerm()).getValue(), shortestLexiconEntry.getDescriptorOffset(), shortestLexiconEntry.getNumBlocks(), blocks);
            if (positionBlockHolder == null) {
                break;
            }
            nextDocId = shortestPosting.getPostings().get(positionBlockHolder.getKey()).getDocId();
        }

        LinkedList<Map.Entry<Integer, Double>> output = new LinkedList<>(finalScores);
        output.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));
        blocks.close();
        return output;
    }

    public static AbstractMap.SimpleEntry<Integer, Integer> nextGEQ(PostingList p, int nextDocId, int position,
                                                                    int block, long descriptorOffset, int numBlocks,
                                                                    FileChannel blockChannel) throws IOException {
        // Variable initialization
        int currentId;
        int nextBlockVal = p.getPostings().get(p.getPostingsLength()-1).getDocId();
        SkippingBlock skippingBlock = new SkippingBlock();

        // Check if the entire posting list has been processed
        if (block == numBlocks) {
            return null;
        }

        for (int i = block; i < numBlocks; i++) {
            if (nextBlockVal < nextDocId) {
                //TODO (check) se siamo qui e per esempio block =0, significa che quello che sto cercando è in 1, perchè non leggo blocco (i+1)?
                skippingBlock = readSkippingBlocks(descriptorOffset + (long) (i+1) * SkippingBlock.getEntrySize(), blockChannel);
                nextBlockVal = skippingBlock.getMaxDocId();
            } else {
                if (i != block) {
                    p.rewritePostings(skippingBlock.retrieveBlock());
                }
                int startPos = (i == block) ? position : 0;
                for (int j = startPos; j < p.getPostingsLength(); j++) {
                    currentId = p.getPostings().get(j).getDocId();
                    if (currentId >= nextDocId) {
                        return new AbstractMap.SimpleEntry<>(j, i);
                    }
                }
            }
        }
        return null;
    }


    public static void main(String[] args) throws IOException {
        //ArrayList<LexiconEntry> entries = new ArrayList();
        String query = "counti new york new york";

        DocumentIndex docIndex = DocumentIndex.getInstance();
        docIndex.loadCollectionStats();
        docIndex.readFromFile();
        //LexiconEntry entry;

        long start = System.currentTimeMillis();
        String processedQuery = Preprocesser.processCLIQuery(query);
        /*for(String term : processedQuery.split(" ")){
            entry = Lexicon.retrieveEntryFromDisk(term);
            entries.add(entry);
        }*/
        System.out.println(MaxScore.maxScoreQuery(processedQuery, 5, true));
        System.out.println(DAATDisjunctive(processedQuery, true, 5));
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: " + timeElapsed);
    }
}
