package it.unipi.mircv.baseStructure;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static it.unipi.mircv.Constants.b;
import static it.unipi.mircv.Constants.k1;

public class LexiconEntry {
        private String term;
        private int df=0;  //document frequency of the term
        private double idf=0; //inverse document frequency of the term
        private int termCollFreq =0; //frequency of the term inside the collection

        private int maxTf=0;   //max term freq inside a doc
        private double maxTfidf=0; //tfidf related to the maxTdf of the term
  
        private double maxBM25=0; //maximum bm25 score of the term
        private long offsetIndexDocId=0;  //offset in the docId file of the inverted index
        private long offsetIndexFreq=0; //offset in the frequency file of the inverted index

        private int docIdSize=0; //size of the term posting list in the docId file of the Inverted index
        private int freqSize=0;  //size of the term posting list in the freq file of the Inverted index

        private long descriptorOffset=0; //starting position of the blockDescriptor into the file
        private int numBlocks=1; //number of blocks to split the list into

        public static final long ENTRY_SIZE_LEXICON = 64+(4+8+4+4+8+8+8+8+4+4+8+4); //64 byte for the term, 4 for int values and 8 for double and long



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
        this.idf = Math.log10(DocumentIndex.getInstance().getCollectionSize()/df);
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
    
    public long getDescriptorOffset() {
        return descriptorOffset;
    }

    public void setDescriptorOffset(long descriptorOffset) {
        this.descriptorOffset = descriptorOffset;
    }

    public int getNumBlocks() {
        return numBlocks;
    }

    public void setNumBlocks(int numBlocks) {
        this.numBlocks = numBlocks;
    }

    public double getMaxBM25() {
        return maxBM25;
    }

    public void setMaxBM25(double maxBM25) {
        this.maxBM25 = maxBM25;
    }

    
    

    /**
     * This function writes the lexiconEntry in the lexiconFile
     * @param positionTerm is the offset to which start to write the stats of the current lexiconEntry
     * @param channelLex is the fileChannel of the lexicon
     * @return the offset to which start to write next lexiconEntry
     * @throws IOException
     */
    public long writeLexiconEntry(long positionTerm, FileChannel channelLex) throws IOException {

        MappedByteBuffer buffer=channelLex.map(FileChannel.MapMode.READ_WRITE,positionTerm, ENTRY_SIZE_LEXICON);

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
        buffer.putDouble(maxBM25);
        buffer.putDouble(idf);
        buffer.putInt(maxTf);

        buffer.putInt(docIdSize);
        buffer.putInt(freqSize);
        buffer.putLong(offsetIndexDocId);
        buffer.putLong(offsetIndexFreq);
        buffer.putLong(descriptorOffset);
        buffer.putInt(numBlocks);

        return positionTerm+ ENTRY_SIZE_LEXICON; //return position from which we have to write

    }

    /**
     * read from disk the entry of the lexicon
     * @param positionTerm the position of the term inside the lexicon
     * @param lexiconFC the file channel from which to read the lexicon
     * @return the lexiconEntry
     * @throws IOException
     */

    public static LexiconEntry readLexEntryFromDisk(long positionTerm, FileChannel lexiconFC) throws IOException {

        ByteBuffer buffer = ByteBuffer.allocate(64);
        lexiconFC.position(positionTerm);

        while (buffer.hasRemaining()) {
            lexiconFC.read(buffer);
        }

        LexiconEntry lexEntry=new LexiconEntry(new String(buffer.array(), StandardCharsets.UTF_8).trim());

        buffer = ByteBuffer.allocate((int) ENTRY_SIZE_LEXICON-64);

        while (buffer.hasRemaining()) {
            lexiconFC.read(buffer);
        }

        buffer.rewind();

        lexEntry.df = buffer.getInt();
        lexEntry.termCollFreq = buffer.getInt();
        lexEntry.maxTfidf = buffer.getDouble();
        lexEntry.maxBM25 = buffer.getDouble();
        lexEntry.idf = buffer.getDouble();
        lexEntry.maxTf = buffer.getInt();

        lexEntry.docIdSize = buffer.getInt();
        lexEntry.freqSize = buffer.getInt();
        lexEntry.offsetIndexDocId = buffer.getLong();
        lexEntry.offsetIndexFreq = buffer.getLong();
        lexEntry.descriptorOffset = buffer.getLong();
        lexEntry.numBlocks = buffer.getInt();

        return lexEntry;
    }



    @Override
    public String toString() {

        return term + "\t" +
                df + ":" + idf + ":" + termCollFreq + ":" + maxTf + ":" + maxTfidf + ":" +maxBM25+":"+ offsetIndexDocId + ":" + offsetIndexFreq + ":" +
                docIdSize + ":" + freqSize + ":"+descriptorOffset+":"+ numBlocks+ "\n";
    }

    /**
     * This function computes the maxBM25 of the term and set the value of the maxBM25 parameter of the LexiconEntry
     * @param postingList the postingList of the term
     */
    public void computeMaxBM25(PostingList postingList) {
        double bm25;
        double avdl = DocumentIndex.getInstance().getAVDL();
        int[] docs = DocumentIndex.getInstance().getDocsLen();
        for (int i = 0; i < postingList.getPostingsLength(); i++) {
            Posting p = postingList.getPostings().get(i);
            //-1 on the docID since in position x of the array there is the length of the doc having docID x-1
            bm25 = ((p.getFrequency()) / (p.getFrequency() + k1 * (1 - b + b * (docs[p.getDocId()-1] / avdl)))*this.idf);
            if (bm25 > this.maxBM25) {
                this.maxBM25 = bm25;
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!LexiconEntry.class.isAssignableFrom(obj.getClass())) {
            return false;
        }
        final LexiconEntry other = (LexiconEntry) obj;
        return this.term.equals(other.term);
    }

}
