package it.unipi.mircv.baseStructure;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static it.unipi.mircv.Constants.PATH_TO_FINAL_DOCINDEX;

public class DocumentIndex {
    private static DocumentIndex instance;
    private int[] docsLen;
    private double AVDL;
    private int collectionSize;


    public static DocumentIndex getInstance(){
        if (instance == null) {
            instance = new DocumentIndex();
        }
        return instance;
    }

    public static void resetInstance(){
        instance = null;

    }
    public double getAVDL() {
        return AVDL;
    }

    public void setAVDL(double AVDL) { DocumentIndex.getInstance().AVDL = AVDL; }

    public int getCollectionSize() {
        return collectionSize;
    }

    public void setCollectionSize(int collectionSize) { DocumentIndex.getInstance().collectionSize = collectionSize; }

    public int[] getDocsLen() {
        return docsLen;
    }

    public void setDocs(int[] docs) { DocumentIndex.getInstance().docsLen = docs; }

    public void addElement(int c){
        docsLen[docsLen.length-1] = c;
    }


    public void readFromFile() throws IOException {
        DocumentIndex docIndex = null;
        try {
            BufferedReader fr = Files.newBufferedReader(Paths.get(PATH_TO_FINAL_DOCINDEX + ".txt"), StandardCharsets.UTF_8);
            docIndex = DocumentIndex.getInstance();
            String firstLine = String.valueOf(fr.readLine());
            String[] firstLineSplit = firstLine.split(":");
            docIndex.setAVDL(Double.parseDouble(firstLineSplit[0]));
            docIndex.setCollectionSize(Integer.parseInt(firstLineSplit[1]));
            docIndex.docsLen = new int[docIndex.getCollectionSize()];
            String line = fr.readLine();
            int i = 0;
            while (line != null) {
                docIndex.docsLen[i] = Integer.parseInt(line);
                line = fr.readLine();
            }
            fr.close();
        } catch (IOException e) {
            System.out.println("Cannot read the DocIndex");
        }
        instance = docIndex;
    }
}