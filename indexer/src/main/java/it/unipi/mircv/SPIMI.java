package it.unipi.mircv;

import it.unipi.mircv.baseStructure.*;
import it.unipi.mircv.compression.VariableByteCompressor;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.prefs.Preferences;
import java.util.stream.Collectors;

import static it.unipi.mircv.Constants.*;
import static it.unipi.mircv.FileUtils.createBuffer;
import static it.unipi.mircv.Preprocesser.process;

public class SPIMI {

    /**
     * This function performs the SPIMI algorithm in order to create the intermediate indexes to be merged
     * @param isCompressed wether the collection is in compressed format or not
     * @throws IOException
     */
    public static void performSpimi(boolean isCompressed) throws IOException {

        //create fileChannel for the intermediate indexes
        if (!Files.exists(Path.of(PATH_TO_INTERMEDIATE_INDEX_FOLDER))) {
            Files.createDirectory(Path.of(PATH_TO_INTERMEDIATE_INDEX_FOLDER));
        }
        else{
            FileUtils.clearFolder(PATH_TO_INTERMEDIATE_INDEX_FOLDER);
        }

        FileUtils.clearFile(PATH_TO_FINAL_DOCINDEX); //clear the file of the final docIndex or create it if it does not exist
        FileUtils.clearFile(PATH_TO_FINAL_DOCNO); //clear the file of the final docNo or create it if it does not exist

        FileChannel docIndexChannel=(FileChannel) Files.newByteChannel(Paths.get(PATH_TO_FINAL_DOCINDEX),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);

        FileChannel docNoChannel=(FileChannel) Files.newByteChannel(Paths.get(PATH_TO_FINAL_DOCNO),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);

        //buffer to the collection file, the second parameter is true if the collection is compressed
        BufferedReader br = createBuffer(PATH_TO_COLLECTION, isCompressed);
        String line;
        String[] docPIDTokens;
        String[] tokens;
        int docID = 1;
        int block_counter = 1;
        boolean terminationFlag = false;
        int docLenAccumulator = 0;

        InvertedIndex invertedIndex;
        ArrayList<Integer> docsLen = new ArrayList<>();
        ArrayList<Integer> docsNo = new ArrayList<>();

        while (!terminationFlag) {
            invertedIndex = InvertedIndex.getInstance();

            //we flush when 80% of the memory is filled
            while (Runtime.getRuntime().freeMemory() > Runtime.getRuntime().totalMemory() * 20 / 100) {
                line = (br.readLine());
                //end of file
                if (line == null) {
                    terminationFlag = true;
                    break;
                }
                line = process(line);
                //split on \t first to get the docID
                docPIDTokens = line.split("\t");
                //if the line is malformed or empty we skip it
                if (docPIDTokens.length == 1 || docPIDTokens[1].isBlank())
                    continue;

                tokens = docPIDTokens[1].split(" ");
                docLenAccumulator += tokens.length;

                for (String token : tokens) {
                    //we either create a new entry for an unseen term or update the existing posting list
                    if (!invertedIndex.getPostingLists().containsKey(token)) {
                        invertedIndex.getPostingLists().put(token, new PostingList(new Posting(docID, 1)));
                    } else {
                        invertedIndex.getPostingLists().get(token).updatePosting(docID);
                    }
                }
                docsLen.add(tokens.length);
                docsNo.add(Integer.parseInt(docPIDTokens[0]));
                System.out.println(docID);
                docID++;
            }

            //sorting the invertedIndex
            invertedIndex.setPostingLists(invertedIndex.getPostingLists().entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (e1, e2) -> e1, LinkedHashMap::new)));
            System.out.println("flushing");
            //saving to disk the intermediate index and the intermediate document index
            flushIndex(invertedIndex.getPostingLists(), block_counter);
            flushDocIndex(docsLen, docsNo, docIndexChannel, docNoChannel);
            block_counter++;

            //clearing the memory and the structures
            InvertedIndex.resetInstance();
            docsLen.clear();
            docsNo.clear();
            System.gc();
        }
        br.close();
        docIndexChannel.close();
        docNoChannel.close();
        DocumentIndex.getInstance().saveCollectionStats((((double) docLenAccumulator) / (docID - 1)), (docID - 1));
    }


    /**
     * Function used to save the docIndex to disk
     * @param docsLen list containing docs length
     * @param docsNo list containing docs number
     * @param docLenChannel channel to the intermediate docsLen file
     * @param docNoChannel channel to the intermediate docsNo file
     */
    private static void flushDocIndex(ArrayList<Integer> docsLen, ArrayList<Integer> docsNo, FileChannel docLenChannel, FileChannel docNoChannel) {
        try{
            docLenChannel.write(ByteBuffer.wrap(VariableByteCompressor.compressArrayInt(docsLen.stream()
                        .mapToInt(Integer::intValue)
                        .toArray())));

            docNoChannel.write(ByteBuffer.wrap(VariableByteCompressor.compressArrayInt(docsNo.stream()
                    .mapToInt(Integer::intValue)
                    .toArray())));

        }catch (IOException e){
            System.out.println("Error in flushing the doc index");
        }
    }

    /**
     * Function used to save the intermediate index to disk
     * @param postings the intermediate index to flush
     * @param numIntermediateIndexes the number of the intermediate index
     * @throws IOException
     */
    private static void flushIndex(HashMap<String, PostingList> postings, int numIntermediateIndexes) throws IOException {
        FileWriter bf = null;
        try {
            //create the channel to the intermediate index file
            bf = new FileWriter(PATH_TO_INTERMEDIATE_INDEX + numIntermediateIndexes + ".txt", StandardCharsets.UTF_8);
            for (Map.Entry<String, PostingList> entry : postings.entrySet()) {
                //the format is term \t docID:frequency docID:frequency ...
                StringBuilder line = new StringBuilder(entry.getKey() + "\t");
                for (Posting p : entry.getValue().getPostings()) {
                    line.append(p.getDocId()).append(":").append(p.getFrequency()).append(" ");
                }
                bf.write(line + "\n");
            }

        } catch (IOException e) {
            System.out.println("Could not flush the index");
        }finally {
            bf.close();}
        }



}
