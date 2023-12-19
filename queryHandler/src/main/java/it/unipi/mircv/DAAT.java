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

public class DAAT {

    /**
     * Function to perform a disjunctive query using the DAAT algorithm
     * @param query the preprocessed query to perform
     * @param k the number of results to return
     * @param isBM25 wether to use BM25 or not
     * @return a list of the k best results
     * @throws IOException if there is an error in reading the block file
     */
    public static LinkedList<Map.Entry<Integer, Double>> DAATDisjunctive(String query, int k, boolean isBM25) throws IOException {
        //variable initialization and transforming the query into a dictionary with the terms and their frequencies
        HashMap<String, Integer> processedQuery = queryToDict(query);
        PriorityQueue<Map.Entry<Integer, Double>> finalScores = new PriorityQueue<>(k, Map.Entry.comparingByValue());
        ArrayList<LexiconEntry> lexiconEntries = new ArrayList<>();

        //retrieving in parallel the lexicon entries from the disk or from the cache
        processedQuery.keySet().parallelStream().forEach(key -> {try {
            LexiconEntry l = LRUCache.retrieveLexEntry(key); //it retrieves the lexiconEntry from the cache if present or from the disk otherwise
            synchronized (lexiconEntries) {
                lexiconEntries.add(l);
            }
        }catch (IOException e) {
            e.printStackTrace();
        }});

        Map<String, AbstractMap.SimpleEntry<Integer, Integer>> positions = processedQuery.keySet().stream()
                .collect(Collectors.toMap(key -> key, key -> new AbstractMap.SimpleEntry<>(0, 0))); //couple which indicates position in the block and numBlock

        DocumentIndex docIndex = DocumentIndex.getInstance();
        ArrayList<PostingList> index = new ArrayList<>();
        FileChannel blocks = (FileChannel) Files.newByteChannel(Paths.get(BLOCK_PATH), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        //sorting the lexicon entries by term in order to have the same order of the postings in the index
        lexiconEntries.sort(Comparator.comparing(LexiconEntry::getTerm));

        //retrieving in parallel the posting lists from the disk or from the cache, keeping the same order of the lexicon entries
        lexiconEntries.parallelStream().forEachOrdered(l -> {try {
            index.add(new PostingList(LRUCache.retrievePostingList(l, blocks)));
        }catch (IOException e) {
            e.printStackTrace();
        }});

        //retrieving the minimum docID among the posting lists
        int nextDocId = minDocID(index, positions);
        double scoreAccumulator;
        AbstractMap.SimpleEntry<Integer, Integer> positionBlockHolder;
        PostingList p;
        long descriptorOffset;
        int numBlocks;

        //iterate until we have processed all the posting lists, which happens when we have a docID equal to Integer.MAX_VALUE
        while(nextDocId != Integer.MAX_VALUE){
            scoreAccumulator = 0;
            for (int j = 0; j < index.size(); ) {

                p = index.get(j);
                Map.Entry<Integer, Integer> termPositions = positions.get(p.getTerm());
                descriptorOffset = lexiconEntries.get(j).getDescriptorOffset();
                numBlocks = lexiconEntries.get(j).getNumBlocks();

                //we move the pointer to the next docID greater or equal than the nextDocId
                positionBlockHolder = nextGEQ(p, nextDocId, termPositions.getKey(),
                termPositions.getValue(), descriptorOffset, numBlocks, blocks);

                //if we get null from nextGEQ, it means that we have finished processing the posting list, so we remove it
                //since it will not give any other result
                if (positionBlockHolder == null) {
                    index.remove(j);
                    lexiconEntries.remove(j);
                    continue;
                }

                //we add the score of the current posting to the accumulator only if the docID is equal to the nextDocId
                Posting postingEntry = p.getPostings().get(positionBlockHolder.getKey());
                if (postingEntry.getDocId() == nextDocId) {

                    scoreAccumulator += scoringFunction(
                            isBM25,
                            processedQuery.get(p.getTerm()),
                            postingEntry.getFrequency(),
                            lexiconEntries.get(j).getIdf(),
                            docIndex.getDocsLen()[nextDocId - 1]
                    );

                    //if we found minDocId, we move the pointer to the next posting so that we don't process it again
                    positionBlockHolder = moveToNext(p, positionBlockHolder.getKey(), positionBlockHolder.getValue(), numBlocks, descriptorOffset, blocks);

                    //again, if we get null from moveToNext, it means that we have finished processing the posting list, so we remove it
                    if (positionBlockHolder == null) {
                        index.remove(j);
                        lexiconEntries.remove(j);
                        continue;
                    }

                }
                //save the new position and block of the posting list
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
            //update nextDocId
            nextDocId = minDocID(index, positions);

        }

        LinkedList<Map.Entry<Integer, Double>> output = new LinkedList<>(finalScores);
        output.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));
        blocks.close();
        return output;
    }

    /**
     * Function to perform a disjunctive query using the DAAT algorithm
     * @param query the preprocessed query to perform
     * @param k the number of results to return
     * @param isBM25 wether to use BM25 or not
     * @return a list of the k best results
     * @throws IOException if there is an error in reading the block file
     */
    public static LinkedList<Map.Entry<Integer, Double>> DAATConjunctive(String query, int k, boolean isBM25) throws IOException {
        //variable initialization and transforming the query into a dictionary with the terms and their frequencies
        HashMap<String, Integer> processedQuery = queryToDict(query);
        PriorityQueue<Map.Entry<Integer, Double>> finalScores = new PriorityQueue<>(k, Map.Entry.comparingByValue());

        Map<String, AbstractMap.SimpleEntry<Integer, Integer>> positions = processedQuery.keySet().stream()
                .collect(Collectors.toMap(key -> key, key -> new AbstractMap.SimpleEntry<>(0, 0))); //couple which indicates positionHolder in the block and numBlock

        DocumentIndex docIndex = DocumentIndex.getInstance();
        ArrayList<PostingList> index = new ArrayList<>();

        ArrayList<LexiconEntry> lexiconEntries = new ArrayList<>();

        FileChannel blocks=(FileChannel) Files.newByteChannel(Paths.get(BLOCK_PATH), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        //retrieving in parallel the lexicon entries from the disk or from the cache
        processedQuery.keySet().parallelStream().forEach(key -> {try {
            LexiconEntry l = LRUCache.retrieveLexEntry(key); //it retrieves the lexiconEntry from the cache if present or from the disk otherwise
            synchronized (lexiconEntries) {
                lexiconEntries.add(l);
            }
        }catch (IOException e) {
            e.printStackTrace();
        }});

        //sorting the lexicon entries by document frequency, so the term having the shortest posting list will be the first
        lexiconEntries.sort(Comparator.comparing(LexiconEntry::getDf));

        //retrieving in parallel the posting lists from the disk or from the cache, keeping the same order of the lexicon entries
        lexiconEntries.parallelStream().forEachOrdered(l -> {try {
            index.add(new PostingList(LRUCache.retrievePostingList(l, blocks)));
        }catch (IOException e) {
            e.printStackTrace();
        }});

        //saving the shortest posting list in order to process the conjunctive query faster
        PostingList shortestPosting = index.get(0);
        LexiconEntry shortestLexiconEntry = lexiconEntries.get(0);
        int nextDocId = shortestPosting.getPostings().get(0).getDocId();
        double scoreAccumulator;
        boolean toCompute;
        AbstractMap.SimpleEntry<Integer, Integer> positionBlockHolder;
        PostingList p = null;



        while(true){
            //we iterate over all the posting lists to check if the docID is present in all of them, this flag
            //is used to specify whether we have to compute the score or not
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

                //if the docID is not present in one of the posting lists, we can stop processing it
                if (p.getPostings().get(positionBlockHolder.getKey()).getDocId() != nextDocId){
                    toCompute = false;
                    break;
                }
        }

            //if the docID is present in all the posting lists, we compute the score and update the finalScores
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

            //we move the pointer to the next posting of the shortest posting list,
            // if we get null it means that we have finished processing it
            positionBlockHolder = moveToNext(shortestPosting, positions.get(shortestLexiconEntry.getTerm()).getKey(),
                    positions.get(shortestLexiconEntry.getTerm()).getValue(), shortestLexiconEntry.getNumBlocks(), shortestLexiconEntry.getDescriptorOffset(), blocks);
            if (positionBlockHolder == null) {
                break;
            }
            positions.put(shortestLexiconEntry.getTerm(), new AbstractMap.SimpleEntry<>(positionBlockHolder.getKey(), positionBlockHolder.getValue()));
            nextDocId = shortestPosting.getPostings().get(positionBlockHolder.getKey()).getDocId();
        }

        LinkedList<Map.Entry<Integer, Double>> output = new LinkedList<>(finalScores);
        output.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));
        blocks.close();
        return output;
    }
}
