package it.unipi.mircv;

import java.util.LinkedList;

public class DocumentIndex {
    private LinkedList<Document> docs;

    public DocumentIndex(){
        docs = new LinkedList<>();
    }

    public LinkedList<Document> getDocs() {
        return docs;
    }

    public void setDocs(LinkedList<Document> docs) {
        this.docs = docs;
    }

    public void addElement(Document c){
        docs.add(c);
    }
}
