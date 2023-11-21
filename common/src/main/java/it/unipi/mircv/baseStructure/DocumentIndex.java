package it.unipi.mircv.baseStructure;

import java.util.ArrayList;

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
}
