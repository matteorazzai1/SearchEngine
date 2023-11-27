package it.unipi.mircv;

import it.unipi.mircv.baseStructure.LexiconEntry;
import it.unipi.mircv.baseStructure.Posting;
import it.unipi.mircv.baseStructure.PostingList;
import it.unipi.mircv.baseStructure.SkippingBlock;
import it.unipi.mircv.compression.UnaryCompressor;
import it.unipi.mircv.compression.VariableByteCompressor;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.nio.file.StandardOpenOption;

import static it.unipi.mircv.Constants.*;


public class MergerProva
{

    public static long positionBlock=0;
    /**
     * It accesses to all the file related to intermediateInvertedIndex at the same time
     * @param args
     * @throws IOException
     * \
     *
     */
    public static void main(String[] args) throws IOException {
        //Obtain all the paths of the intermediateIndex
        ArrayList<String> filePaths=new ArrayList<>();

        try {
            File file = new File(BLOCK_DEBUG_PATH);
            FileWriter fileWriter = new FileWriter(file,false);
            BufferedWriter writer = new BufferedWriter(fileWriter);


            writer.close(); // Close the writer to save changes

        } catch (IOException e) {
            e.printStackTrace();
        }

        //TODO togliere commento a questa riga sotto, eliminando quella successiva
        //for(int i=0; i<numIntermediateIndexes;i++){
        for(int i=1;i<4;i++){
            filePaths.add("indexer/data/pathToOutput"+i+".txt");
        }

        // Create a thread pool to process the files concurrently
        ExecutorService executor = Executors.newFixedThreadPool(filePaths.size());


        List<Future<?>> futures = new ArrayList<>();

        PriorityQueue<PostingList> intermediateIndex=new PriorityQueue<>(Comparator.comparing(PostingList::getTerm));
        for (String filePath : filePaths) {

            Future<?> future = executor.submit(() ->  {
                // Synchronize access to the intermediateIndex
                synchronized (intermediateIndex) {
                    processDatFile(filePath, intermediateIndex);
                }
            });
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
                //System.out.println(postingList);

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
        LinkedHashMap<String, LexiconEntry> finalLexicon=new LinkedHashMap<>();
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
                for(Posting post:intermediatePostingList.getPostings()){ //we have to count only the new posting for a specific term
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
                lexEntry.setDf(intermediatePostingList.getPostings().size()); //length of posting list is the total number of document in which the term is present
                lexEntry.setIdf(lexEntry.getDf()); //we pass the df, and in the setIdf method we compute the idf

                //start taking term collection frequency and maxTf
                int freq_term=lexEntry.getTermCollFreq();

                int maxTf=lexEntry.getMaxTf();
                for(Posting post:intermediatePostingList.getPostings()){ //we have to count only the new posting for a specific term
                    freq_term+=post.getFrequency();

                    lexEntry.setTermCollFreq(freq_term);
                    if(post.getFrequency()>maxTf){
                        maxTf=post.getFrequency();
                        lexEntry.setMaxTf(maxTf);
                        lexEntry.setMaxTfidf(maxTf); //in the setMaxTfidf it will compute the MaxTfidf
                    }
                }

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

        String pathDocId="indexer/data/inv_index_docId.dat";
        String pathFreq="indexer/data/inv_index_freq.dat";

        try {

            FileChannel docIdChannel=(FileChannel) Files.newByteChannel(Paths.get(pathDocId),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.CREATE);

            FileChannel freqsChannel=(FileChannel) Files.newByteChannel(Paths.get(pathFreq),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.CREATE);

            FileChannel blockChannel=(FileChannel) Files.newByteChannel(Paths.get(BLOCK_PATH),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.CREATE);

            long offsetDocId=0;
            long offsetFreq=0;

            for(Map.Entry<String,PostingList> entry:finalIndex.entrySet()) {

                PostingList finalPostingList=new PostingList(entry.getValue().toString());

                int[] docIds=new int[finalPostingList.getPostings().size()];   //number of posting will be also the number of freqs and docIds
                int[] freqs=new int[finalPostingList.getPostings().size()];

                int postingPos=0;

                //construct the array of docId and freqs
                for(Posting post: finalPostingList.getPostings()){

                    docIds[postingPos]=post.getDocId();
                    freqs[postingPos]=post.getFrequency();
                    postingPos++;

                }
                /*byte[] compressedDocId= VariableByteCompressor.compressArrayInt(docIds);
                byte[] compressedFreq=UnaryCompressor.compressArrayInt(freqs);
                docIdChannel.write(ByteBuffer.wrap(compressedDocId));
                freqsChannel.write(ByteBuffer.wrap(compressedFreq));*/



                int block_size = (int) Math.ceil(Math.sqrt(finalPostingList.getPostings().size()));
                int num_blocks = (int) Math.ceil((double)finalPostingList.getPostings().size()/block_size);

                int docIdSize=0;
                int freqSize=0;

                LexiconEntry lexEntry=finalLexicon.get(finalPostingList.getTerm());

                lexEntry.setDescriptorOffset(positionBlock);
                lexEntry.setNumBlocks(num_blocks);

                ArrayList<Integer> docIdsBlock;
                ArrayList<Integer> freqBlock;


                for(int currentBlock=0; currentBlock<num_blocks; currentBlock++) {


                    docIdsBlock=new ArrayList<>();
                    freqBlock=new ArrayList<>();


                    for (int j = 0; j < block_size; j++) {
                        if (currentBlock * block_size + j < finalPostingList.getPostings().size()) {
                            docIdsBlock.add(docIds[currentBlock * block_size + j]);
                            freqBlock.add(freqs[currentBlock * block_size + j]);
                        }
                    }


                    byte[] compressedDocId = VariableByteCompressor.compressArrayInt(docIdsBlock.stream()
                            .mapToInt(Integer::intValue)
                            .toArray());
                    byte[] compressedFreq = UnaryCompressor.compressArrayInt(freqBlock.stream()
                            .mapToInt(Integer::intValue)
                            .toArray());
                    docIdChannel.write(ByteBuffer.wrap(compressedDocId));
                    freqsChannel.write(ByteBuffer.wrap(compressedFreq));



                    //create skipping block

                    SkippingBlock skippingBlock=new SkippingBlock(docIdsBlock.get(docIdsBlock.size()-1),offsetDocId,compressedDocId.length,offsetFreq,compressedFreq.length,docIdsBlock.size());
                    positionBlock=skippingBlock.writeSkippingBlock(positionBlock, blockChannel);
                    skippingBlock.writeDebugSkippingBlock(finalPostingList.getTerm());

                    offsetDocId += compressedDocId.length;
                    offsetFreq += compressedFreq.length;
                    docIdSize+=compressedDocId.length;
                    freqSize+=compressedFreq.length;

                }

                //set offset inside lexiconEntry

                if(lexEntry!=null) {
                    lexEntry.setOffsetIndexDocId(offsetDocId);
                    lexEntry.setOffsetIndexFreq(offsetFreq);
                    lexEntry.setDocIdSize(docIdSize);
                    lexEntry.setFreqSize(freqSize);
                }

            }
            docIdChannel.close(); // Close the writer to save changes
            freqsChannel.close(); // Close the writer to save changes

            List<String> inputArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();

            boolean isDebugging = inputArguments.toString().contains("-agentlib:jdwp");

            if (isDebugging) {
                System.out.println("Debugging mode");
                saveMergedIndexDebugging(finalIndex,finalLexicon);
            } else {
                System.out.println("Not in debugging mode");
                writeLexicon(finalLexicon);
            }
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


    private static void saveMergedIndexDebugging(LinkedHashMap<String, PostingList> finalIndex, LinkedHashMap<String, LexiconEntry> finalLexicon) {
        String path="indexer/data/invIndex_debug.txt";

        try {
            File file = new File(path);
            FileWriter fileWriter = new FileWriter(file);
            BufferedWriter writer = new BufferedWriter(fileWriter);

            for(Map.Entry<String,PostingList> entry:finalIndex.entrySet()) {
                writer.write(entry.getValue().toString());
            }
            writer.close(); // Close the writer to save changes

            System.out.println("Data has been written to " + path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        path="indexer/data/lexicon_debug.txt";

        try {
            File file = new File(path);
            FileWriter fileWriter = new FileWriter(file);
            BufferedWriter writer = new BufferedWriter(fileWriter);

            for(Map.Entry<String, LexiconEntry> entry:finalLexicon.entrySet()) {
                writer.write(entry.getValue().toString());
            }
            writer.close(); // Close the writer to save changes

            System.out.println("Data has been written to " + path);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}