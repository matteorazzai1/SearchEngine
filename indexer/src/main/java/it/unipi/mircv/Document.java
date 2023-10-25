package it.unipi.mircv;

public class Document {
    private int docNo;
    private int docID;
    private int length;

    public Document(int docNo, int docID, int length) {
        this.docNo = docNo;
        this.docID = docID;
        this.length = length;
    }

    public int getDocNo() {
        return docNo;
    }

    public void setDocNo(int docNo) {
        this.docNo = docNo;
    }

    public int getDocID() {
        return docID;
    }

    public void setDocID(int docID) {
        this.docID = docID;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }
}
