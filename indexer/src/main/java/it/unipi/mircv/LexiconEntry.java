package it.unipi.mircv;

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class LexiconEntry {
        private String term;
        private int df=0;  //document frequency of the term
        private double idf=0; //inverse document frequency of the term
        private int termCollFreq =0; //frequency of the term inside the collection

        private int maxTf=0;   //max term freq inside a doc
        private double maxTfidf=0; //tfidf related to the maxTdf of the term
        private long offsetIndexDocId=0;  //offset in the docId file of the inverted index
        private long offsetIndexFreq=0; //offset in the frequency file of the inverted index

        private int docIdSize=0; //size of the term posting list in the docId file of the Inverted index
        private int freqSize=0;  //size of the term posting list in the freq file of the Inverted index
        private double CollectionSize=1000;  //TODO we have to delete it and substitute with the right collectionsize

        private static final long ENTRY_SIZE = 64+(4+8+4+4+8+8+8+4+4); //64 byte for the term, 4 for int values and 8 for double and long

    /**
     * Constructor of the LexiconEntry
     *
     */
    public LexiconEntry(String term){
        this.term=term;
    }

    public String getTerm() {
        return term;
    }

    public int getDf() {
        return df;
    }

    public double getIdf() {
        return idf;
    }

    public int getDocIdSize() {
        return docIdSize;
    }

    public int getFreqSize() {
        return freqSize;
    }

    public int getTermCollFreq() {
        return termCollFreq;
    }

    public double getMaxTfidf() {
        return maxTfidf;
    }

    public int getMaxTf() {
        return maxTf;
    }

    public long getOffsetIndexDocId() {
        return offsetIndexDocId;
    }

    public long getOffsetIndexFreq() {
        return offsetIndexFreq;
    }

    public void setDf(int df) {
        this.df = df;
    }
    public void setIdf(double df) {
        this.idf = Math.log10(CollectionSize/df); //TODO we have to set the right collectionSize
    }

    public void setDocIdSize(int docIdSize) {
        this.docIdSize = docIdSize;
    }

    public void setFreqSize(int freqSize) {
        this.freqSize = freqSize;
    }

    public void setOffsetIndexDocId(long offsetIndexDocId) {
        this.offsetIndexDocId = offsetIndexDocId;
    }

    public void setOffsetIndexFreq(long offsetIndexFreq) {
        this.offsetIndexFreq = offsetIndexFreq;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public void setTermCollFreq(int termCollFreq) {
        this.termCollFreq = termCollFreq;
    }

    public void setMaxTfidf(int maxTf) {
        this.maxTfidf = (1+Math.log10(maxTf))*this.getIdf();
    }
    public void setMaxTf(int maxTf) {
        this.maxTf=maxTf;
    }


    /**
     * This function writes the lexiconEntry in the lexiconFile
     * @param positionTerm is the offset to which start to write the stats of the current lexiconEntry
     * @param channelLex is the fileChannel of the lexicon
     * @return the offset to which start to write next lexiconEntry
     * @throws IOException
     */
    public long writeLexiconEntry(long positionTerm, FileChannel channelLex) throws IOException {

        MappedByteBuffer buffer=channelLex.map(FileChannel.MapMode.READ_WRITE,positionTerm,ENTRY_SIZE);

        CharBuffer charBuffer = CharBuffer.allocate(64);

            for (int i = 0; i < term.length(); i++) {
                if(i<64) {
                    charBuffer.put(i, term.charAt(i));
                }
            }

        buffer.put(StandardCharsets.UTF_8.encode(charBuffer));

        buffer.putInt(df);
        buffer.putInt(termCollFreq);
        buffer.putDouble(maxTfidf);
        buffer.putDouble(idf);
        buffer.putInt(maxTf);

        buffer.putInt(docIdSize);
        buffer.putInt(freqSize);
        buffer.putLong(offsetIndexDocId);
        buffer.putLong(offsetIndexFreq);

        return positionTerm+ENTRY_SIZE; //return position from which we have to write

    }


}
