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

import static it.unipi.mircv.Constants.*;


public class Merger
{

    static long offsetDocId=0;
    static long offsetFreq=0;
    static long positionTerm=0;
    /**
     * It accesses to all the file related to intermediateInvertedIndex at the same time
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        //Obtain all the paths of the intermediateIndex
        ArrayList<String> filePaths=new ArrayList<>();
        LinkedHashMap<String,PostingList> finalIndex=new LinkedHashMap<>();
        LinkedHashMap<String,LexiconEntry> finalLexicon=new LinkedHashMap<>();

        //TODO togliere commento a questa riga sotto, eliminando quella successiva
        //for(int i=0; i<Constants.block_number;i++){
        for(int i=1;i<4;i++){
            filePaths.add("indexer/data/pathToOutput"+i+".txt");
        }


        PriorityQueue<PostingList> intermediateIndex=new PriorityQueue<>(Comparator.comparing(PostingList::getTerm));

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

            mergePostingList(getMinPostings(intermediateIndex), finalIndex, finalLexicon);
        }


        //control intermediateIndex sia vuoto
        if(!intermediateIndex.isEmpty()){
            while(!intermediateIndex.isEmpty()){
                System.out.println(intermediateIndex.isEmpty());
                mergePostingList(getMinPostings(intermediateIndex), finalIndex, finalLexicon);
            }
        }
        //if we are here we have to save the last part of the LinkedHashMap that we did not save before, because this part
        //did not full the memory yet
        saveMergedIndex(finalIndex,finalLexicon);
    }

    /**
     * it takes the first term in lexicographically order inside the intermediateIndex and return all the postingLists of that term
     * @param intermediateIndex priorityQueue in which we insert progressively the posting list of a single line of each intermediateIndex
     * @return PriorityQueue of the postingList of that term
     * @throws IOException
     */

    private static PriorityQueue<PostingList> getMinPostings(PriorityQueue<PostingList> intermediateIndex) throws IOException {

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
     * @param intermediateIndex is the priority queue with all the postingList in lexicographical order of term
     * @param finalIndex
     * @param finalLexicon
     * @throws IOException
     */
    private static void mergePostingList(PriorityQueue<PostingList> intermediateIndex, LinkedHashMap<String, PostingList> finalIndex, LinkedHashMap<String, LexiconEntry> finalLexicon) throws IOException {

        PostingList intermediatePostingList=intermediateIndex.poll();

        if(intermediatePostingList!=null) {
            PostingList finalPostingList = new PostingList(intermediatePostingList.getTerm(), intermediatePostingList.getPostings());
            LexiconEntry lexEntry = new LexiconEntry(intermediatePostingList.getTerm());


            while (!intermediateIndex.isEmpty()) {
                //we have to merge
                finalPostingList.appendList(intermediateIndex.poll()); //this insert the posting in order way (respect to docId)
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
            finalIndex.put(finalPostingList.getTerm(), finalPostingList);
            finalLexicon.put(lexEntry.getTerm(), lexEntry);
            if(controlMemory()){
                //the memory is going to become full
                saveMergedIndex(finalIndex, finalLexicon);
                finalIndex.clear();
                finalLexicon.clear();
            }
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
                byte[] compressedDocId= VariableByteCompressor.compressArrayInt(docIds);
                byte[] compressedFreq=UnaryCompressor.compressArrayInt(freqs);
                docIdChannel.write(ByteBuffer.wrap(compressedDocId));
                freqsChannel.write(ByteBuffer.wrap(compressedFreq));

                //set offset inside lexiconEntry
                LexiconEntry lexEntry=finalLexicon.get(finalPostingList.getTerm());
                if(lexEntry!=null) {
                    lexEntry.setOffsetIndexDocId(offsetDocId);
                    lexEntry.setOffsetIndexFreq(offsetFreq);
                    lexEntry.setDocIdSize(compressedDocId.length);
                    lexEntry.setFreqSize(compressedFreq.length);
                }
                offsetDocId+=compressedDocId.length;
                offsetFreq+=compressedFreq.length;

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


        FileChannel lexicon=(FileChannel) Files.newByteChannel(Paths.get(LEXICON_PATH),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);

        for(LexiconEntry lex: finalLexicon.values()){
            positionTerm=lex.writeLexiconEntry(positionTerm,lexicon);
        }

    }


    private static void saveMergedIndexDebugging(LinkedHashMap<String, PostingList> finalIndex, LinkedHashMap<String, LexiconEntry> finalLexicon) {


        try {
            File file = new File(INV_INDEX_DEBUG);
            FileWriter fileWriter = new FileWriter(file);
            BufferedWriter writer = new BufferedWriter(fileWriter);

            for(Map.Entry<String,PostingList> entry:finalIndex.entrySet()) {
                writer.write(entry.getValue().toString());
            }
            writer.close(); // Close the writer to save changes

        } catch (IOException e) {
            e.printStackTrace();
        }



        try {
            File file = new File(LEXICON_DEBUG);
            FileWriter fileWriter = new FileWriter(file);
            BufferedWriter writer = new BufferedWriter(fileWriter);

            for(Map.Entry<String, LexiconEntry> entry:finalLexicon.entrySet()) {
                writer.write(entry.getValue().toString());
            }
            writer.close(); // Close the writer to save changes

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
