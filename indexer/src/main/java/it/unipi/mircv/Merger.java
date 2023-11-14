package it.unipi.mircv;

import it.unipi.mircv.compression.UnaryCompressor;
import it.unipi.mircv.compression.VariableByteCompressor;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static it.unipi.mircv.Constants.*;


public class Merger
{

    private static long offsetDocId=0;
    private static long offsetFreq=0;
    private static long positionTerm=0;
    private static PriorityQueue<PostingList> intermediateIndex=new PriorityQueue<>(Comparator.comparing(PostingList::getTerm));

    private static FileChannel docIdChannel=null;

    private static FileChannel freqsChannel=null;

    private static FileChannel lexiconChannel=null;

    static boolean isDebugging = false;

    /**
     * It accesses to all the file related to intermediateInvertedIndex at the same time
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        //check if we are in debugging mode or not
        List<String> inputArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();

        isDebugging = inputArguments.toString().contains("-agentlib:jdwp");

        if(isDebugging){
                //clear file for debug
               FileUtils.clearDebugFiles();
        }

        //Obtain all the paths of the intermediateIndex
        ArrayList<String> filePaths=new ArrayList<>();

        //TODO togliere commento a questa riga sotto, eliminando quella successiva
        //for(int i=0; i<Constants.block_number;i++){
        for(int i=1;i<4;i++){
            filePaths.add("indexer/data/pathToOutput"+i+".txt");
        }

        //open file channels

        docIdChannel=(FileChannel) Files.newByteChannel(Paths.get(INV_INDEX_DOCID),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);

        freqsChannel=(FileChannel) Files.newByteChannel(Paths.get(INV_INDEX_FREQS),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);

        lexiconChannel=(FileChannel) Files.newByteChannel(Paths.get(LEXICON_PATH),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);


        List<BufferedReader> bufferedReaderList=new ArrayList<>();
        for (String filePath : filePaths) {

            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            bufferedReaderList.add(reader);
        }

        int i=0;

        Iterator<BufferedReader> iterator = bufferedReaderList.iterator();

        while (!bufferedReaderList.isEmpty()) {
            i++;
            System.out.println("riga" + i);

            while (iterator.hasNext()) {
                BufferedReader reader = iterator.next();
                String line = reader.readLine();

                if (line != null) {
                    PostingList postingList = new PostingList(line);
                    intermediateIndex.add(postingList);
                } else {
                    iterator.remove(); // Remove the current reader from the list
                }
            }

            // Reset the iterator for the next iteration
            iterator = bufferedReaderList.iterator();

            mergePostingList(getMinPostings());
        }


        //control intermediateIndex sia vuoto
        if(!intermediateIndex.isEmpty()){
            while(!intermediateIndex.isEmpty()){
                System.out.println(intermediateIndex.isEmpty());
                mergePostingList(getMinPostings());
            }
        }

        docIdChannel.close(); // Close the writer to save changes
        freqsChannel.close(); // Close the writer to save changes
    }


    /**
     * it takes the first term in lexicographically order inside the intermediateIndex and return all the postingLists of that term
     * @return PriorityQueue of the postingList of that term
     * @throws IOException
     */

    private static PriorityQueue<PostingList> getMinPostings() throws IOException {

        PriorityQueue<PostingList> matchingQueue = new PriorityQueue<>(Comparator.comparing(PostingList::getTerm));

        if (!intermediateIndex.isEmpty()) {
            PostingList firstPostingList = intermediateIndex.peek();
            String firstTerm = firstPostingList.getTerm();

            // Collect and add all PostingLists with the same term to the new PriorityQueue
            intermediateIndex.stream()
                    .filter(postingList -> postingList.getTerm().equals(firstTerm))
                    .forEach(matchingQueue::add);

            intermediateIndex.removeIf(postingList -> postingList.getTerm().equals(firstTerm));

        }
        return matchingQueue;
    }

    /**
     * this function makes the merge of the postingList of the same terms to generate the finalIndex
     *
     * @param minTermIndex is the priority queue with all the postingList of the first term in the intermediateIndex in lexicographical order
     * @throws IOException
     */
    private static void mergePostingList(PriorityQueue<PostingList> minTermIndex) throws IOException {

        PostingList intermediatePostingList=minTermIndex.poll();

        if(intermediatePostingList!=null) {
            PostingList finalPostingList = new PostingList(intermediatePostingList.getTerm(), intermediatePostingList.getPostings());
            LexiconEntry lexEntry = new LexiconEntry(intermediatePostingList.getTerm());


            while (!minTermIndex.isEmpty()) {
                //we have to merge
                finalPostingList.appendList(minTermIndex.poll()); //this insert the posting in order way (respect to docId)
            }

            ////we have to add statistic of the term on the lexicon file
            lexEntry.setDf(finalPostingList.getPostings().size()); //length of posting list is the total number of document in which the term is present
            lexEntry.setIdf(lexEntry.getDf()); //we pass the df, and in the setIdf method we compute the idf

            int freq_term = lexEntry.getTermCollFreq();
            int maxTf = lexEntry.getMaxTf();
            for (Posting post : intermediatePostingList.getPostings()) { //we have to count only the new posting for a specific term
                freq_term += post.getFrequency();
                lexEntry.setTermCollFreq(freq_term);
                if (post.getFrequency() > maxTf) {
                    maxTf = post.getFrequency();
                    lexEntry.setMaxTf(maxTf);
                    lexEntry.setMaxTfidf(maxTf); //in the setMaxTfidf it will compute the MaxTfidf
                }
            }
            saveMergedIndex(finalPostingList,lexEntry);
        }
    }

    private static boolean controlMemory() {
        // Define a threshold for memory usage (80% in this example)
        double memoryThreshold = 0.8;

        // Get current memory information
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemoryUsage = memoryBean.getHeapMemoryUsage();

        // Calculate the percentage of used memory
        double usedMemoryPercentage = (double) heapMemoryUsage.getUsed() / heapMemoryUsage.getMax();

        // Check if memory usage is approaching the threshold
        if (usedMemoryPercentage > memoryThreshold) {
            return true;
        }
        else{
            return false;
        }

    }

    /**
     * this function makes two operations:
     *              (i) takes the LinkedHashMap of the finalIndex and wrote it on two different files, one for the DocId of the posting lists,
     *                  and one for the Freq of the posting lists.
     *              (ii) add the offsets within the invertedIndex files from which the posting lists of the specific term start
     * @param finalPostingList
     * @param lexEntry
     */
    private static void saveMergedIndex(PostingList finalPostingList,LexiconEntry lexEntry) throws IOException {


            int[] docIds=new int[finalPostingList.getPostings().size()];   //number of posting will be also the number of freqs and docIds
            int[] freqs=new int[finalPostingList.getPostings().size()];

            int postingPos=0;

            //construct the array of docId and freqs
            for(Posting post: finalPostingList.getPostings()){

                docIds[postingPos]=post.getDocId();
                freqs[postingPos]=post.getFrequency();
                postingPos++;

            }
            byte[] compressedDocId= VariableByteCompressor.compressArrayInt(docIds);
            byte[] compressedFreq=UnaryCompressor.compressArrayInt(freqs);
            docIdChannel.write(ByteBuffer.wrap(compressedDocId));
            freqsChannel.write(ByteBuffer.wrap(compressedFreq));

            //set offset inside lexiconEntry

            if(lexEntry!=null) {
                lexEntry.setOffsetIndexDocId(offsetDocId);
                lexEntry.setOffsetIndexFreq(offsetFreq);
                lexEntry.setDocIdSize(compressedDocId.length);
                lexEntry.setFreqSize(compressedFreq.length);
            }
            offsetDocId+=compressedDocId.length;
            offsetFreq+=compressedFreq.length;



            if (isDebugging) {
                //System.out.println("Debugging mode");
                saveMergedIndexDebugging(finalPostingList,lexEntry);
            } else {
                //System.out.println("Not in debugging mode");
                if(lexEntry!=null)
                    positionTerm=lexEntry.writeLexiconEntry(positionTerm,lexiconChannel);
            }
            //System.out.println("Data has been written to " + path);

    }

    /**
     * This function takes all the lexiconEntry related to the finalIndex and wrote that on the lexicon file
     * @param lex  lexiconEntry of that term
     * @throws IOException
     */
    private static void writeLexicon(LexiconEntry lex) throws IOException {


        FileChannel lexicon=(FileChannel) Files.newByteChannel(Paths.get(LEXICON_PATH),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);


        positionTerm=lex.writeLexiconEntry(positionTerm,lexicon);


    }


    private static void saveMergedIndexDebugging(PostingList finalIndex, LexiconEntry finalLexicon) {


        try {
            File file = new File(INV_INDEX_DEBUG);
            FileWriter fileWriter = new FileWriter(file,true);
            BufferedWriter writer = new BufferedWriter(fileWriter);


            writer.write(finalIndex.toString());

            writer.close(); // Close the writer to save changes

        } catch (IOException e) {
            e.printStackTrace();
        }



        try {
            File file = new File(LEXICON_DEBUG);
            FileWriter fileWriter = new FileWriter(file,true);
            BufferedWriter writer = new BufferedWriter(fileWriter);


            writer.write(finalLexicon.toString());

            writer.close(); // Close the writer to save changes

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
