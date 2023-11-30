package it.unipi.mircv.baseStructure;

import it.unipi.mircv.compression.UnaryCompressor;
import it.unipi.mircv.compression.VariableByteCompressor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;

import static it.unipi.mircv.Constants.*;

public class SkippingBlock {

    private int maxDocId;

    private long offsetDocId;

    private int docIdSize;

    private long offsetFreq;

    private int freqSize;

    private int numPostings;

    private static final int BLOCK_ENTRY_SIZE= 4+8+4+8+4+4;

    @Override
    public String toString() {
        return "Block info : " +
                "maxDocId = " + maxDocId +
                ", offsetDocId = " + offsetDocId +
                ", docIdSize = " + docIdSize +
                ", offsetFreq = " + offsetFreq +
                ", freqSize = " + freqSize +
                ", numPostings = " + numPostings +"\n";
    }


    public SkippingBlock(int maxDocId,long offsetDocId, int docIdSize,long offsetFreq, int freqSize, int numPostings) {
        this.maxDocId = maxDocId;
        this.offsetDocId=offsetDocId;
        this.docIdSize=docIdSize;
        this.offsetFreq=offsetFreq;
        this.freqSize=freqSize;
        this.numPostings=numPostings;
    }

    public SkippingBlock(){};

    public int getMaxDocId() {
        return maxDocId;
    }

    public void setMaxDocId(int maxDocId) {
        this.maxDocId = maxDocId;
    }

    public long getOffsetDocId() {
        return offsetDocId;
    }

    public void setOffsetDocId(long offsetDocId) {
        this.offsetDocId = offsetDocId;
    }

    public int getDocIdSize() {
        return docIdSize;
    }

    public void setDocIdSize(int docIdSize) {
        this.docIdSize = docIdSize;
    }

    public long getOffsetFreq() {
        return offsetFreq;
    }

    public void setOffsetFreq(long offsetFreq) {
        this.offsetFreq = offsetFreq;
    }

    public int getFreqSize() {
        return freqSize;
    }

    public void setFreqSize(int freqSize) {
        this.freqSize = freqSize;
    }

    public int getNumPostings() {
        return numPostings;
    }

    public void setNumPostings(int numPostings) {
        this.numPostings = numPostings;
    }

    public static int getEntrySize(){
        return BLOCK_ENTRY_SIZE;
    }

    /**
     * write a single blockDescriptor on file
     * @param positionBlock
     * @return
     * @throws IOException
     */
    public long writeSkippingBlock(long positionBlock, FileChannel blocks) throws IOException {


        MappedByteBuffer buffer=blocks.map(FileChannel.MapMode.READ_WRITE,positionBlock, BLOCK_ENTRY_SIZE);


        buffer.putInt(maxDocId);
        buffer.putLong(offsetDocId);
        buffer.putInt(docIdSize);
        buffer.putLong(offsetFreq);
        buffer.putInt(freqSize);
        buffer.putInt(numPostings);


        return positionBlock+ BLOCK_ENTRY_SIZE; //return position from which we have to write

    }

    /**
     * read single block from file
     * @param positionBlock position of the block inside the file
     * @throws IOException
     */
    public static SkippingBlock readSkippingBlocks(long positionBlock, FileChannel blocks) throws IOException {

        SkippingBlock b = new SkippingBlock();
        MappedByteBuffer buffer=blocks.map(FileChannel.MapMode.READ_WRITE, positionBlock, BLOCK_ENTRY_SIZE);

        b.setMaxDocId(buffer.getInt());
        b.setOffsetDocId(buffer.getLong());
        b.setDocIdSize(buffer.getInt());
        b.setOffsetFreq(buffer.getLong());
        b.setFreqSize(buffer.getInt());
        b.setNumPostings(buffer.getInt());

        return b;
    }

    /**
     * retrieve the postings of the block
     * @return the list of posting of the block
     * @throws IOException
     */
    public ArrayList<Posting> retrieveBlock() throws IOException {

        ArrayList<Posting> blockPostingList=new ArrayList<>();

        FileChannel docIdChannel=(FileChannel) Files.newByteChannel(Paths.get(INV_INDEX_DOCID),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);

        FileChannel freqChannel=(FileChannel) Files.newByteChannel(Paths.get(INV_INDEX_FREQS),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);

        MappedByteBuffer bufferDoc=docIdChannel.map(FileChannel.MapMode.READ_WRITE,offsetDocId,docIdSize);
        MappedByteBuffer bufferFreq=freqChannel.map(FileChannel.MapMode.READ_WRITE,offsetFreq,freqSize);

        if(bufferDoc==null || bufferFreq==null){
                return null;
        }

        byte[] compressedDocIds=new byte[docIdSize];
        byte[] compressedFreq=new byte[freqSize];

        bufferDoc.get(compressedDocIds,0,docIdSize);
        bufferFreq.get(compressedFreq,0,freqSize);

        int[] decompressedArrayDocId= VariableByteCompressor.decompressArray(compressedDocIds,numPostings);
        int[] decompressedArrayFreq= UnaryCompressor.decompressArrayInt(compressedFreq,numPostings);

        for(int i=0;i<numPostings;i++){
                Posting posting=new Posting(decompressedArrayDocId[i],decompressedArrayFreq[i]);
                blockPostingList.add(posting);
        }

        docIdChannel.close();
        freqChannel.close();
        return blockPostingList;
    }

    public void writeDebugSkippingBlock(String term) {

        try {
            File file = new File(BLOCK_DEBUG_PATH);
            FileWriter fileWriter = new FileWriter(file,true);
            BufferedWriter writer = new BufferedWriter(fileWriter);


            writer.write(term+" "+this.toString());

            writer.close(); // Close the writer to save changes

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
