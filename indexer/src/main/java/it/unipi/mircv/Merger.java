package it.unipi.mircv;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class Merger
{
    public static void main(String[] args) {
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

    private static void mergePostingList(PriorityQueue<PostingList> intermediateIndex) {

        LinkedHashMap<String,PostingList> finalIndex=new LinkedHashMap<>();
        while(!intermediateIndex.isEmpty()){
            PostingList intermediatePostingList=intermediateIndex.poll();

            PostingList finalPostingList=finalIndex.get(intermediatePostingList.term);
            if(finalPostingList!=null){
                //we have to merge
                finalPostingList.appendList(intermediatePostingList); //this insert the posting in order way (respect to docId)
            }
            else{
                //we insert the complete postingList
                finalIndex.put(intermediatePostingList.term,intermediatePostingList);
            }
        }

        saveMergedIndex(finalIndex);


    }

    private static void saveMergedIndex(LinkedHashMap<String, PostingList> finalIndex) {

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
                //writer.write(entry.getValue().toString());
                System.out.println(entry.getValue().toString());
                PostingList finalPostingList=new PostingList(entry.getValue().toString());
                writerDocId.write(finalPostingList.getTerm());
                writerFreq.write(finalPostingList.getTerm());

                for(Posting post: finalPostingList.getPostings()){
                    writerDocId.write(" "+post.getDocId());
                    writerFreq.write(" "+post.getFrequency());
                }
                writerDocId.write("\n");
                writerFreq.write("\n");
            }
            writerDocId.close(); // Close the writer to save changes
            writerFreq.close(); // Close the writer to save changes

            //System.out.println("Data has been written to " + path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
