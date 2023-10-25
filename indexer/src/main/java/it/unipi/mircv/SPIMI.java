package it.unipi.mircv;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SPIMI {
    public static void performIndexing(String path, boolean isCompressed, boolean isDebug) throws IOException {
        BufferedReader br = createBuffer(path, isCompressed);
        String line;
        String[] docPIDTokens;
        String[] tokens;
        int docID = 1;
        boolean terminationFlag = false;

        while (!terminationFlag) {

            InvertedIndex invertedIndex = new InvertedIndex();
            DocumentIndex docIndex = new DocumentIndex();

            while (Runtime.getRuntime().freeMemory() > Runtime.getRuntime().totalMemory() * 20 / 100) {
                line = br.readLine();
                if (line == null) {
                    terminationFlag = true;
                    break;
                }
                if (line.isBlank())
                    continue;
                docPIDTokens = line.split("\t"); //split on \t first to get the docID
                tokens = docPIDTokens[1].split(" ");

                for (String token : tokens) {

                    if (!invertedIndex.getPostingLists().containsKey(token)) {
                        invertedIndex.getPostingLists().put(token, new PostingList(new Posting(docID, 1)));
                    } else {
                        invertedIndex.getPostingLists().get(token).updatePosting(docID);
                    }
                }
                Document toInsert = new Document(Integer.parseInt(docPIDTokens[0]), docID, tokens.length);
                docIndex.addElement(toInsert);
                docID++;
            }
            invertedIndex.setPostingLists(invertedIndex.getPostingLists().entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (e1, e2) -> e1, LinkedHashMap::new)));
            flushIndex(invertedIndex.getPostingLists(), isDebug);
            flushLexicon(invertedIndex.getPostingLists());

        }
        br.close();
    }


    private static void flushLexicon(HashMap<String, PostingList> postings) throws IOException {
        try (FileWriter bf = new FileWriter("pathToLexiconOutput.txt")) {
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
        }
    }

        private static void flushIndex(HashMap<String, PostingList> postings, boolean isDebug) throws IOException {
        try (FileWriter bf = new FileWriter("pathToOutput.txt")) {
            for (Map.Entry<String, PostingList> entry : postings.entrySet()) {
                StringBuilder line = new StringBuilder(entry.getKey() + "\t");
                for (Posting p : entry.getValue().getPostings()) {
                    line.append(p.getDocId()).append(":").append(p.getFrequency()).append(" ");
                }
                bf.write(line + "\n");
            }

        } catch (IOException e) {
            System.out.println("Could not flush the index");
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
        SPIMI.performIndexing("collection.tar.gz", true, false);
        /*
        FileWriter bf = new FileWriter("data.txt");
        String line = "1\t" + "ciao mi chiamo domenico\n";
        String line2 = "2\t" + "molto piacere io sono edoardo\n";
        String line3 = "3\t" + "domenico piacere mio di conoscerti\n";
        String line4 = "4\t" + "ciao sono molto contento di conoscerti domenico\n";
        String line5 = "5\t" + "ciao ciao contento molto molto domenico\n";

        bf.write(line);
        bf.write(line2);
        bf.write(line3);
        bf.write(line4);
        bf.write(line5);
        bf.close();
        */
    }
}
