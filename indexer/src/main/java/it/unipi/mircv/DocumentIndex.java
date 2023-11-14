package it.unipi.mircv;

import java.util.ArrayList;
import java.util.LinkedList;

public class DocumentIndex {
    private static ArrayList<Document> docs;
    private static double AVDL;
    private static int collectionSize;

    public DocumentIndex(){
        docs = new ArrayList<>();
    }

    public static double getAVDL() {
        return AVDL;
    }

    public static void setAVDL(double AVDL) {
        DocumentIndex.AVDL = AVDL;
    }

    public static int getCollectionSize() {
        return collectionSize;
    }

    public static void setCollectionSize(int collectionSize) {
        DocumentIndex.collectionSize = collectionSize;
    }

    public static ArrayList<Document> getDocs() {
        return docs;
    }

    public void setDocs(ArrayList<Document> docs) {
        DocumentIndex.docs = docs;
    }

    public void addElement(Document c){
        docs.add(c);
    }
}
