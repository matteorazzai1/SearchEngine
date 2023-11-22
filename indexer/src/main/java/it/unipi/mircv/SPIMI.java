package it.unipi.mircv;

import it.unipi.mircv.baseStructure.*;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static it.unipi.mircv.Constants.*;
import static it.unipi.mircv.Preprocesser.process;

public class SPIMI {
    public static void performIndexing(boolean isCompressed) throws IOException {
        BufferedReader br = createBuffer(PATH_TO_COLLECTION, isCompressed);
        String line;
        String[] docPIDTokens;
        String[] tokens;
        int docID = 1;
        int block_counter = 1;
        boolean terminationFlag = false;
        int docLenAccumulator = 0;

        InvertedIndex invertedIndex;
        DocumentIndex docIndex;


        while (!terminationFlag) {
            invertedIndex = InvertedIndex.getInstance();
            docIndex = DocumentIndex.getInstance();


            while (Runtime.getRuntime().freeMemory() > Runtime.getRuntime().totalMemory() * 20 / 100) {
                line = (br.readLine());
                if (line == null) {
                    terminationFlag = true;
                    break;
                }
                line = process(line);
                docPIDTokens = line.split("\t"); //split on \t first to get the docID
                if (docPIDTokens.length == 1 || docPIDTokens[1].isBlank())
                    continue;

                tokens = docPIDTokens[1].split(" ");
                docLenAccumulator += tokens.length;

                for (String token : tokens) {
                    if (!invertedIndex.getPostingLists().containsKey(token)) {
                        invertedIndex.getPostingLists().put(token, new PostingList(new Posting(docID, 1)));
                    } else {
                        invertedIndex.getPostingLists().get(token).updatePosting(docID);
                    }
                }
                Document toInsert = new Document(Integer.parseInt(docPIDTokens[0]), docID, tokens.length);
                docIndex.addElement(toInsert);
                System.out.println(docID);
                docID++;
            }
            invertedIndex.setPostingLists(invertedIndex.getPostingLists().entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (e1, e2) -> e1, LinkedHashMap::new)));
            System.out.println("flushing");
            flushIndex(invertedIndex.getPostingLists(), block_counter);
            flushDocIndex(docIndex, block_counter);
            block_counter++;
            invertedIndex.resetInstance();
            DocumentIndex.resetInstance();
            System.gc();
        }
        br.close();
        Constants.numIntermediateIndexes = block_counter;
        addFirstLineDocIndexData((((double) docLenAccumulator) / (docID - 1)) + ":" + (docID - 1));
    }

    private static void addFirstLineDocIndexData(String i) {
        try {
            Path path = Paths.get(PATH_TO_INTERMEDIATE_DOCINDEX + "1.txt");
            List<String> existingLines = Files.readAllLines(path);
            existingLines.add(0, i);
            Files.write(path, existingLines);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void flushDocIndex(DocumentIndex docIndex, int block_counter) {
        try{
            FileWriter bf = new FileWriter(PATH_TO_INTERMEDIATE_DOCINDEX + block_counter + ".txt", StandardCharsets.UTF_8);
            for (Document d : docIndex.getDocs()){
                bf.write(d.toString() + "\n");
            }
            bf.close();
        }catch (IOException e){
            System.out.println("Error in flushing the doc index");
        }
    }

    private static void flushLexicon(HashMap<String, PostingList> postings, int block_counter) throws IOException {
        FileWriter bf = null;
        try{
            bf = new FileWriter(PATH_TO_INTERMEDIATE_LEXICON + block_counter + ".txt", StandardCharsets.UTF_8);
            int df;
            int maxTF;
            StringBuilder s;
            for (Map.Entry<String, PostingList> entry : postings.entrySet()){
                s = new StringBuilder();
                maxTF = 0;
                PostingList p = entry.getValue();
                df = p.getPostings().size();
                for (Posting posting : p.getPostings()){
                    if(posting.getFrequency() > maxTF){
                        maxTF = posting.getFrequency();
                    }
                }
                s.append(entry.getKey()).append("\t").append(maxTF).append(":").append(df);
                bf.write(s + "\n");

            }
        } catch (IOException e){
            System.out.println("Error in flushing the lexicon");
        }finally {
            bf.close();
        }
    }

        private static void flushIndex(HashMap<String, PostingList> postings, int numIntermediateIndexes) throws IOException {

        FileWriter bf = null;
        try {
            bf = new FileWriter(PATH_TO_INTERMEDIATE_INDEX + numIntermediateIndexes + ".txt", StandardCharsets.UTF_8);
            for (Map.Entry<String, PostingList> entry : postings.entrySet()) {
                StringBuilder line = new StringBuilder(entry.getKey() + "\t");
                for (Posting p : entry.getValue().getPostings()) {
                    line.append(p.getDocId()).append(":").append(p.getFrequency()).append(" ");
                }
                bf.write(line + "\n");
            }

        } catch (IOException e) {
            System.out.println("Could not flush the index");
        }finally {
            bf.close();
        }


    }

    public static BufferedReader createBuffer(String path, boolean isCompressed) throws IOException {
        if (isCompressed) {
            TarArchiveInputStream tarInput = new TarArchiveInputStream
                    (new GzipCompressorInputStream(new FileInputStream(path)));
            tarInput.getNextTarEntry();
            return new BufferedReader(new InputStreamReader(tarInput, StandardCharsets.UTF_8));
        }
        return Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8);

    }

    public static void main(String[] args) throws IOException {
        //performIndexing(true);
        DocumentIndex.readFromFile();
    }
}
