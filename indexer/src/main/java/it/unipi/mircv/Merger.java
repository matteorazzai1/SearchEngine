package it.unipi.mircv;

import it.unipi.mircv.baseStructure.LexiconEntry;
import it.unipi.mircv.baseStructure.Posting;
import it.unipi.mircv.baseStructure.PostingList;
import it.unipi.mircv.baseStructure.SkippingBlock;
import it.unipi.mircv.compression.UnaryCompressor;
import it.unipi.mircv.compression.VariableByteCompressor;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.nio.file.StandardOpenOption;

import static it.unipi.mircv.Constants.*;


public class Merger
{

    private static long offsetDocId=0;
    private static long offsetFreq=0;
    private static long positionTerm=0;

    private static long positionBlock=0;

    private static FileChannel docIdChannel=null;

    private static FileChannel freqsChannel=null;

    private static FileChannel lexiconChannel=null;

    private static FileChannel blockChannel=null;

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

        
        for(int i = 1; i< numIntermediateIndexes; i++){
            System.out.println("Arrivo qui");
            filePaths.add(PATH_TO_INTERMEDIATE_INDEX+i+".txt");
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

        blockChannel=(FileChannel) Files.newByteChannel(Paths.get(BLOCK_PATH),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);

        // Define a HashMap to hold readers and the current line for each file
        HashMap<BufferedReader, PostingList> readerLines = new HashMap<>();

        // Populate the initial lines for each reader
        for (String filePath : filePaths) {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line = reader.readLine();
            if (line != null) {
                readerLines.put(reader, new PostingList(line));
            }
        }

        int i=0;

        while (!readerLines.isEmpty()) {

            i++;
            if(i%10000==0)
                System.out.println("riga" + i);

            // Get min Term
            String minTerm = findMinTerm(readerLines);

            PostingList minPosting=new PostingList(minTerm,new ArrayList<Posting>());

            Iterator<Map.Entry<BufferedReader, PostingList>> iterator = readerLines.entrySet().iterator();

            // Get all readers with the same term
            while(iterator.hasNext()) {
                Map.Entry<BufferedReader, PostingList> entry = iterator.next();
                PostingList postingList = entry.getValue();
                if (postingList.getTerm().equals(minTerm)) {
                    //we are inside a reader with the min term
                    minPosting.appendList(postingList);

                    BufferedReader reader = entry.getKey();
                    String line = reader.readLine();
                    if (line != null) {
                        readerLines.put(reader, new PostingList(line));
                    } else {
                        iterator.remove(); // Remove the current reader from the list
                    }
                }
            }
            saveMergedIndex(minPosting);

        }


        docIdChannel.close(); // Close the writer to save changes
        freqsChannel.close(); // Close the writer to save changes
    }


    public static String findMinTerm(HashMap<BufferedReader, PostingList> map) {
        String minTerm = null;

        for (PostingList postingList : map.values()) {
            String term = postingList.getTerm();
            if (minTerm == null || term.compareTo(minTerm) < 0) {
                minTerm = term;
            }
        }

        return minTerm;
    }



    /**
     * this function makes two operations:
     *              (i) takes the LinkedHashMap of the finalIndex and wrote it on two different files, one for the DocId of the posting lists,
     *                  and one for the Freq of the posting lists.
     *              (ii) add the offsets within the invertedIndex files from which the posting lists of the specific term start
     * @param finalPostingList
     */
    private static void saveMergedIndex(PostingList finalPostingList) throws IOException {

            LexiconEntry lexEntry = new LexiconEntry(finalPostingList.getTerm());

            ////we have to add statistic of the term on the lexicon file
            lexEntry.setDf(finalPostingList.getPostings().size()); //length of posting list is the total number of document in which the term is present
            lexEntry.setIdf(lexEntry.getDf()); //we pass the df, and in the setIdf method we compute the idf

            int freq_term = lexEntry.getTermCollFreq();
            int maxTf = lexEntry.getMaxTf();


            int[] docIds=new int[finalPostingList.getPostings().size()];   //number of posting will be also the number of freqs and docIds
            int[] freqs=new int[finalPostingList.getPostings().size()];

            int postingPos=0;

            //construct the array of docId and freqs
            for(Posting post: finalPostingList.getPostings()){

                docIds[postingPos]=post.getDocId();
                freqs[postingPos]=post.getFrequency();
                postingPos++;

                //set lexEntry parameters
                freq_term += post.getFrequency();
                lexEntry.setTermCollFreq(freq_term);
                if (post.getFrequency() > maxTf) {
                    maxTf = post.getFrequency();
                    lexEntry.setMaxTf(maxTf);
                    lexEntry.setMaxTfidf(maxTf); //in the setMaxTfidf it will compute the MaxTfidf
                }

            }

            int block_size = (int) Math.ceil(Math.sqrt(finalPostingList.getPostings().size()));
            int num_blocks = (int) Math.ceil((double)finalPostingList.getPostings().size()/block_size);

            int docIdSize=0;
            int freqSize=0;

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
                if(isDebugging)
                    skippingBlock.writeDebugSkippingBlock(finalPostingList.getTerm());

                offsetDocId += compressedDocId.length;
                offsetFreq += compressedFreq.length;
                docIdSize+=compressedDocId.length;
                freqSize+=compressedFreq.length;

            }

                //set offset inside lexiconEntry

            if (lexEntry != null) {
                lexEntry.setOffsetIndexDocId(offsetDocId-docIdSize);
                lexEntry.setOffsetIndexFreq(offsetFreq-freqSize);
                lexEntry.setDocIdSize(docIdSize);
                lexEntry.setFreqSize(freqSize);
                lexEntry.computeMaxBM25(finalPostingList); //compute the maxBM25 of the term
            }


            if (isDebugging) {
                //System.out.println("Debugging mode");
                saveMergedIndexDebugging(finalPostingList, lexEntry);
            } else {
                //System.out.println("Not in debugging mode");
                if (lexEntry != null)
                    positionTerm = lexEntry.writeLexiconEntry(positionTerm, lexiconChannel);
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
