package it.unipi.mircv;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.nio.file.StandardOpenOption;


public class Merger
{
    /**
     * It accesses to all the file related to intermediateInvertedIndex at the same time
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        // Replace these with the paths to your .dat files
        String[] filePaths = {"indexer/data/file1.dat", "indexer/data/file2.dat"};

        // Create a thread pool to process the files concurrently
        ExecutorService executor = Executors.newFixedThreadPool(filePaths.length);


        List<Future<?>> futures = new ArrayList<>();

        PriorityQueue<PostingList> intermediateIndex=new PriorityQueue<>(Comparator.comparing(PostingList::getTerm));
        for (String filePath : filePaths) {

            Future<?> future = executor.submit(() -> processDatFile(filePath, intermediateIndex));
            futures.add(future);
        }

        // Wait for all threads to complete
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Shut down the executor
        executor.shutdown();

        //merging intermediate

        mergePostingList(intermediateIndex);

    }

    /**
     * it goes to process the file taken as input
     * @param filePath path of the related invertedIndex
     * @param intermediateIndex Priority queue in which all the PostingList of the terms of the intermediateIndex are stored, in order
     *                          to be processed to merge all the postingList of the same terms to generate the finalIndex
     */
    private static void processDatFile(String filePath, PriorityQueue<PostingList> intermediateIndex) {


        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;

            while ((line = reader.readLine()) != null) {

                    PostingList postingList=new PostingList(line);

                    intermediateIndex.add(postingList);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * this function makes the merge of the postingList of the same terms to generate the finalIndex
     * @param intermediateIndex is the priority queue with all the postingList in lexicographical order of term
     * @throws IOException
     */
    private static void mergePostingList(PriorityQueue<PostingList> intermediateIndex) throws IOException {

        LinkedHashMap<String,PostingList> finalIndex=new LinkedHashMap<>();
        LinkedHashMap<String,LexiconEntry> finalLexicon=new LinkedHashMap<>();
        while(!intermediateIndex.isEmpty()){
            PostingList intermediatePostingList=intermediateIndex.poll();

            PostingList finalPostingList=finalIndex.get(intermediatePostingList.term);
            if(finalPostingList!=null){
                //we have to merge
                finalPostingList.appendList(intermediatePostingList); //this insert the posting in order way (respect to docId)
                ////we have to add statistic of the term on the lexicon file
                LexiconEntry lexEntry=finalLexicon.get(intermediatePostingList.term);
                lexEntry.setDf(finalPostingList.getPostings().size()); //length of posting list is the total number of document in which the term is present
                lexEntry.setIdf(lexEntry.getDf()); //we pass the df, and in the setIdf method we compute the idf

                int freq_term=lexEntry.getTermCollFreq();
                int maxTf=lexEntry.getMaxTf();
                for(Posting post:finalPostingList.getPostings()){
                        freq_term+=post.getFrequency();
                        lexEntry.setTermCollFreq(freq_term);
                        if(post.getFrequency()>maxTf){
                                maxTf=post.getFrequency();
                                lexEntry.setMaxTf(maxTf);
                                lexEntry.setMaxTfidf(maxTf); //in the setMaxTfidf it will compute the MaxTfidf
                        }
                }

            }
            else{
                //we insert the complete postingList
                finalIndex.put(intermediatePostingList.term,intermediatePostingList);
                //we have to add statistic of the term on the lexicon file
                LexiconEntry lexEntry=new LexiconEntry(intermediatePostingList.term);
                //lexEntry.setDf(intermediatePostingList.getPostings().size()); //TODO:Do we need to set partial length here? I don't think so

                finalLexicon.put(intermediatePostingList.term,lexEntry);
            }
        }

        saveMergedIndex(finalIndex,finalLexicon);


    }

    /**
     * this function makes two operations:
     *              (i) takes the LinkedHashMap of the finalIndex and wrote it on two different files, one for the DocId of the posting lists,
     *                  and one for the Freq of the posting lists.
     *              (ii) add the offsets within the invertedIndex files from which the posting lists of the specific term start
     * @param finalIndex
     * @param finalLexicon
     */
    private static void saveMergedIndex(LinkedHashMap<String, PostingList> finalIndex, LinkedHashMap<String, LexiconEntry> finalLexicon) {

        String pathDocId="indexer/data/file_final_docId.dat";
        String pathFreq="indexer/data/file_final_freq.dat";

        try {
            File fileDocId = new File(pathDocId);
            FileWriter fileWriterDocId = new FileWriter(fileDocId);
            BufferedWriter writerDocId = new BufferedWriter(fileWriterDocId);

            File fileFreq = new File(pathFreq);
            FileWriter fileWriterFreq = new FileWriter(fileFreq);
            BufferedWriter writerFreq = new BufferedWriter(fileWriterFreq);

            for(Map.Entry<String,PostingList> entry:finalIndex.entrySet()) {

                PostingList finalPostingList=new PostingList(entry.getValue().toString());
                System.out.println(finalPostingList.getTerm());
                writerDocId.write(finalPostingList.getTerm());
                writerFreq.write(finalPostingList.getTerm());

                //TODO compression of docIdList and FreqList for the postingList of each term
                //TODO after compression, set the right offset to the lexiconEntry of the term
                long offsetDocId=0;
                long offsetFreq=0;
                for(Posting post: finalPostingList.getPostings()){
                    writerDocId.write(" "+post.getDocId());
                    writerFreq.write(" "+post.getFrequency());
                    offsetDocId+=4; //hypothesis of 4 byte (int) for each docid TODO correct with right values after compression
                    offsetFreq+=4; //hypothesis of 4 byte (int) for each docid TODO correct with right values after compression
                }

                //set offset inside lexiconEntry
                System.out.println(finalPostingList.getTerm());
                LexiconEntry lexEntry=finalLexicon.get(finalPostingList.getTerm());
                if(lexEntry!=null) {
                    lexEntry.setOffsetIndexDocId(offsetDocId);
                    lexEntry.setOffsetIndexFreq(offsetFreq);
                }

                writerDocId.write("\n");
                writerFreq.write("\n");
            }
            writerDocId.close(); // Close the writer to save changes
            writerFreq.close(); // Close the writer to save changes

            writeLexicon(finalLexicon);
            //System.out.println("Data has been written to " + path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This function takes all the lexiconEntry related to the finalIndex and wrote that on the lexicon file
     * @param finalLexicon is the LinkedHashMap composed by term, and lexiconEntry of that term
     * @throws IOException
     */
    private static void writeLexicon(LinkedHashMap<String, LexiconEntry> finalLexicon) throws IOException {

        String lexiconPath="indexer/data/lexicon.dat";

        FileChannel lexicon=(FileChannel) Files.newByteChannel(Paths.get(lexiconPath),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);

        long positionTerm=0;
        for(LexiconEntry lex: finalLexicon.values()){
            positionTerm=lex.writeLexiconEntry(positionTerm,lexicon);
        }

    }

}
