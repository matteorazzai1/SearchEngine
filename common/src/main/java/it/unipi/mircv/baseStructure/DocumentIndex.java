package it.unipi.mircv.baseStructure;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import static it.unipi.mircv.Constants.PATH_TO_FINAL_DOCINDEX;

public class DocumentIndex {
    private static DocumentIndex instance;
    private ArrayList<Document> docs;
    private double AVDL;
    private int collectionSize;

    private DocumentIndex(){
        docs = new ArrayList<>();
    }

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

    public ArrayList<Document> getDocs() {
        return docs;
    }

    public void setDocs(ArrayList<Document> docs) { DocumentIndex.getInstance().docs = docs; }

    public void addElement(Document c){
        docs.add(c);
    }


    public static DocumentIndex readFromFile() throws IOException {
        DocumentIndex docIndex = null;
        try {
            BufferedReader fr = Files.newBufferedReader(Paths.get(PATH_TO_FINAL_DOCINDEX + ".txt"), StandardCharsets.UTF_8);
            docIndex = DocumentIndex.getInstance();
            String firstLine = String.valueOf(fr.readLine());
            String[] firstLineSplit = firstLine.split(":");
            docIndex.setAVDL(Double.parseDouble(firstLineSplit[0]));
            docIndex.setCollectionSize(Integer.parseInt(firstLineSplit[1]));
            String line = fr.readLine();
            while (line != null) {
                String[] lineSplit = line.split(":");
                docIndex.addElement(new Document(Integer.parseInt(lineSplit[0]), Integer.parseInt(lineSplit[1]), Integer.parseInt(lineSplit[2])));
                line = fr.readLine();
            }
            fr.close();
        } catch (IOException e) {
            System.out.println("Cannot read the DocIndex");
        }
        return docIndex;
    }
}