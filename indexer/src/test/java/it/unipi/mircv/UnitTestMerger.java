package it.unipi.mircv;
import it.unipi.mircv.baseStructure.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;


public class UnitTestMerger {
    private static FileChannel docIdChannel = null;
    private static FileChannel freqsChannel = null;

    public static Object[] performUnitTestMerger(ArrayList<String> filePaths, int[] docLens) throws IOException {
        ArrayList<ArrayList<int[]>> index = new ArrayList<>();
        Lexicon lexicon = new Lexicon();
        HashMap<String, LexiconEntry> lexiconMap = new HashMap<>();
        /*docIdChannel = (FileChannel) Files.newByteChannel(Paths.get("indexer/test/data/inv_index_docId_test.txt"),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);

        freqsChannel = (FileChannel) Files.newByteChannel(Paths.get("indexer/test/data/inv_index_freq_test.txt"),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);*/

        HashMap<BufferedReader, PostingList> readerLines = new HashMap<>();

        for (String filePath : filePaths) {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line = reader.readLine();
            if (line != null) {
                readerLines.put(reader, new PostingList(line));
            }
        }

        while (!readerLines.isEmpty()) {
            String minTerm = findMinTermTest(readerLines);
            PostingList minPosting = new PostingList(minTerm, new ArrayList<>());
            Iterator<Map.Entry<BufferedReader, PostingList>> iterator = readerLines.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<BufferedReader, PostingList> entry = iterator.next();
                PostingList postingList = entry.getValue();
                if(postingList.getTerm().equals(minTerm)) {
                    minPosting.appendList(postingList);
                    BufferedReader reader = entry.getKey();
                    String line = reader.readLine();
                    if (line != null) {
                        //System.out.println("line: "+line);
                        readerLines.put(reader, new PostingList(line));
                    } else {
                        iterator.remove();
                    }
                }
            }
            index.add((ArrayList<int[]>) SaveMergedIndexTest(minPosting, docLens)[0]);
            lexiconMap.put(minTerm, (LexiconEntry) SaveMergedIndexTest(minPosting, docLens)[1]);

        }
        //docIdChannel.close();
        //freqsChannel.close();
        lexicon.setLexicon(lexiconMap);
        Object[] results = new Object[2];
        results[0] = index;
        results[1] = lexicon;
        return results;
    }

    public static String findMinTermTest(HashMap<BufferedReader, PostingList> map) {
        String minTerm = null;

        for (PostingList postingList : map.values()) {
            String term = postingList.getTerm();
            if (minTerm == null || term.compareTo(minTerm) < 0) {
                minTerm = term;
            }
        }

        return minTerm;
    }

    public static Object[] SaveMergedIndexTest(PostingList finalPostingList, int[] docLens) throws IOException{
        ArrayList<int[]> resultsIndex = new ArrayList<>();
        DocumentIndex docIndex = DocumentIndex.getInstance();
        docIndex.setDocs(docLens);
        LexiconEntry lexEntry = new LexiconEntry(finalPostingList.getTerm());
        lexEntry.setDf(finalPostingList.getPostings().size());
        lexEntry.setIdf(lexEntry.getDf());
        int freqTerm = lexEntry.getTermCollFreq();
        int maxTf = lexEntry.getMaxTf();
        int[] docIds = new int[finalPostingList.getPostings().size()];
        int[] freqs = new int[finalPostingList.getPostings().size()];
        int postingPosition = 0;
        for (Posting posting : finalPostingList.getPostings()) {
            docIds[postingPosition] = posting.getDocId();
            freqs[postingPosition] = posting.getFrequency();
            postingPosition++;
            freqTerm += posting.getFrequency();
            lexEntry.setTermCollFreq(freqTerm);
            if (posting.getFrequency() > maxTf) {
                maxTf = posting.getFrequency();
                lexEntry.setMaxTf(maxTf);
                lexEntry.setMaxTfidf(maxTf);
            }
        }
        lexEntry.computeMaxBM25(finalPostingList);
        /*int block_size;
        int num_blocks;
        if(finalPostingList.getPostings().size() <= 256){
            block_size= finalPostingList.getPostings().size();
            num_blocks = 1;
        }
        else{
            block_size = (int) Math.ceil(Math.sqrt(finalPostingList.getPostings().size()));
            num_blocks = (int) Math.ceil((double)finalPostingList.getPostings().size()/block_size);
        }

        int docIdSize=0;
        int freqSize=0;

        lexEntry.setNumBlocks(num_blocks);

        ArrayList<Integer> docIdsBlock;
        ArrayList<Integer> freqBlock;
        for(int currentBlock=0; currentBlock<num_blocks; currentBlock++) {

            ArrayList<ArrayList<Integer>> current = new ArrayList<>();
            docIdsBlock=new ArrayList<>();
            freqBlock=new ArrayList<>();


            for (int j = 0; j < block_size; j++) {
                if (currentBlock * block_size + j < finalPostingList.getPostings().size()) {
                    docIdsBlock.add(docIds[currentBlock * block_size + j]);
                    freqBlock.add(freqs[currentBlock * block_size + j]);
                }
            }

            /*docIdChannel.write(ByteBuffer.wrap(compressedDocId));
            freqsChannel.write(ByteBuffer.wrap(compressedFreq));

            current.add(docIdsBlock);
            current.add(freqBlock);
            //System.out.println("current "+currentBlock+": "+current);
            results.add(current);
        }*/
        resultsIndex.add(docIds);
        resultsIndex.add(freqs);
        Object[] results = new Object[2];
        results[0] = resultsIndex;
        results[1] = lexEntry;
        return results;
    }

}
