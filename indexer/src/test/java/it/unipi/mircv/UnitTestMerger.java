package it.unipi.mircv;
import it.unipi.mircv.baseStructure.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;


public class UnitTestMerger {
    //a test version of the merger
    public static Object[] performUnitTestMerger(ArrayList<String> filePaths, int[] docLens) throws IOException {
        ArrayList<ArrayList<int[]>> index = new ArrayList<>(); //arraylist for the final index
        Lexicon lexicon = new Lexicon(); //final lexicon
        HashMap<String, LexiconEntry> lexiconMap = new HashMap<>(); //hashmap for the final lexicon

        HashMap<BufferedReader, PostingList> readerLines = new HashMap<>(); //hashmap for the buffered readers

        for (String filePath : filePaths) { //for each file path
            BufferedReader reader = new BufferedReader(new FileReader(filePath)); //create a buffered reader
            String line = reader.readLine(); //read a line
            if (line != null) { //if the line is not null
                readerLines.put(reader, new PostingList(line)); //put the buffered reader and a new posting list in the hashmap
            }
        }

        while (!readerLines.isEmpty()) { //while the hashmap is not empty
            String minTerm = findMinTermTest(readerLines); //find the minimum term
            PostingList minPosting = new PostingList(minTerm, new ArrayList<>()); //create a new posting list with the minimum term
            //create an iterator to iterate over the hashmap
            Iterator<Map.Entry<BufferedReader, PostingList>> iterator = readerLines.entrySet().iterator();
            while (iterator.hasNext()) { //while the iterator has a next element
                Map.Entry<BufferedReader, PostingList> entry = iterator.next(); //get the next element
                PostingList postingList = entry.getValue(); //get the posting list
                if(postingList.getTerm().equals(minTerm)) { //if the posting list term is equal to the minimum term
                    minPosting.appendList(postingList); //append the posting list to the posting list of the minimum term
                    BufferedReader reader = entry.getKey(); //get the buffered reader
                    String line = reader.readLine(); //read a line from the buffered reader
                    if (line != null) { //if the line is not null
                        readerLines.put(reader, new PostingList(line)); //put the buffered reader and a new posting list in the hashmap
                    } else { //if the line is null
                        iterator.remove(); //remove the buffered reader from the hashmap
                    }
                }
            }
            Object[] results = SaveMergedIndexTest(minPosting, docLens); //save the results of the merged index
            index.add((ArrayList<int[]>) results[0]); //save the merged index
            lexiconMap.put(minTerm, (LexiconEntry) SaveMergedIndexTest(minPosting, docLens)[1]); //save the lexicon

        }
        lexicon.setLexicon(lexiconMap); //set the lexicon
        Object[] results = new Object[2]; //create an array for the results
        results[0] = index;
        results[1] = lexicon;
        return results;
    }

    //a method to find the minimum term
    public static String findMinTermTest(HashMap<BufferedReader, PostingList> map) {
        String minTerm = null;
        //iterate over the hashmap
        for (PostingList postingList : map.values()) { //for each posting list
            String term = postingList.getTerm(); //get the term
            if (minTerm == null || term.compareTo(minTerm) < 0) { //if the term is less than the minimum term or the minimum term is null
                minTerm = term; //set the minimum term to the term
            }
        }

        return minTerm;
    }

    //a method to save the merged index
    public static Object[] SaveMergedIndexTest(PostingList finalPostingList, int[] docLens) throws IOException{
        ArrayList<int[]> resultsIndex = new ArrayList<>(); //arraylist for the final index
        DocumentIndex docIndex = DocumentIndex.getInstance(); //get the document index
        docIndex.setDocs(docLens); //set the documents lengths
        LexiconEntry lexEntry = new LexiconEntry(finalPostingList.getTerm()); //create a new lexicon entry with the term of the input posting list
        lexEntry.setDf(finalPostingList.getPostings().size()); //set the document frequency
        lexEntry.setIdf(lexEntry.getDf()); //set the inverse document frequency
        int freqTerm = lexEntry.getTermCollFreq(); //get the term collection frequency from the lexicon entry
        int maxTf = lexEntry.getMaxTf(); //get the maximum term frequency from the lexicon entry
        int[] docIds = new int[finalPostingList.getPostings().size()]; //array for the document IDs
        int[] freqs = new int[finalPostingList.getPostings().size()]; //array for the frequencies
        int postingPosition = 0; // initialize the position for the posting
        for (Posting posting : finalPostingList.getPostings()) { //for each posting in the input posting list
            docIds[postingPosition] = posting.getDocId(); //save the document ID
            freqs[postingPosition] = posting.getFrequency(); //save the frequency
            postingPosition++; //update the posting position
            freqTerm += posting.getFrequency(); //update the term collection frequency
            lexEntry.setTermCollFreq(freqTerm); //set the term collection frequency
            if (posting.getFrequency() > maxTf) { //if the posting frequency is greater than the maximum term frequency
                maxTf = posting.getFrequency(); //update the maximum term frequency
                lexEntry.setMaxTf(maxTf); //set the maximum term frequency in the lexicon entry
                lexEntry.setMaxTfidf(maxTf); //set the maximum tfidf in the lexicon entry
            }
        }
        lexEntry.computeMaxBM25(finalPostingList); //compute the maximum BM25 in the lexicon entry
        //add docIds and freqs to the results index
        resultsIndex.add(docIds);
        resultsIndex.add(freqs);
        Object[] results = new Object[2]; //create an array for the final results
        results[0] = resultsIndex;
        results[1] = lexEntry;
        return results;
    }
}
