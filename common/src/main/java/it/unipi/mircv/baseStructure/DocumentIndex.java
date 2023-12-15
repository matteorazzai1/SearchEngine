package it.unipi.mircv.baseStructure;

import it.unipi.mircv.FileUtils;
import it.unipi.mircv.compression.VariableByteCompressor;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.prefs.Preferences;

import static it.unipi.mircv.Constants.*;

public class DocumentIndex {
    private static DocumentIndex instance;

    //documents <pid>
    private int[] docsNo;

    //length of documents
    private int[] docsLen;

    //average length of documents
    private double AVDL;

    //number of documents in the collection
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

    public int[] getDocsNo() {
        return docsNo;
    }

    public void setDocsNo(int[] docsNo) {
        this.docsNo = docsNo;
    }


    /**
     * Reads the docIndex from file
     * @throws IOException if the file cannot be read
     */
    public void readFromFile() throws IOException {

        instance.docsLen = new int[instance.getCollectionSize()];

        FileChannel docIndexChannel=(FileChannel) Files.newByteChannel(Paths.get(PATH_TO_FINAL_DOCINDEX + ".txt"),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);
        try {
            MappedByteBuffer buffer=docIndexChannel.map(FileChannel.MapMode.READ_WRITE,0, FileUtils.retrieveFileSize(PATH_TO_FINAL_DOCINDEX+ ".txt"));

            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);

            instance.docsLen = VariableByteCompressor.decompressArray(bytes);
            docIndexChannel.close();
        } catch (IOException e) {
            System.out.println("Cannot read the DocIndex");
        }

    }

    /**
     * Saves the collection stats using the Preferences API
     * @throws IOException if the file cannot be written
     */
    public void saveCollectionStats(Double AVDL, int collectionSize) {
        Preferences prefs = Preferences.userNodeForPackage(DocumentIndex.class);
        prefs.put("AVDL", String.valueOf(AVDL));
        prefs.put("collectionSize", String.valueOf(collectionSize));
    }

    /**
     * Loads the collection stats using the Preferences API
     * @throws IOException if the file cannot be read
     */
    public void loadCollectionStats() {
        Preferences prefs = Preferences.userNodeForPackage(DocumentIndex.class);
        this.setAVDL(Double.parseDouble(prefs.get("AVDL", "")));
        this.setCollectionSize(Integer.parseInt(prefs.get("collectionSize", "")));
    }


    /**
     * Reads the docNo from file
     * @throws IOException if the file cannot be read
     */
    public void retrieveDocsNo() throws IOException {

        instance.docsNo = new int[instance.getCollectionSize()];

        FileChannel docNoChannel=(FileChannel) Files.newByteChannel(Paths.get(PATH_TO_FINAL_DOCNO + ".txt"),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);
        try {
            //opens the channel to the file
            MappedByteBuffer buffer=docNoChannel.map(FileChannel.MapMode.READ_WRITE,0, FileUtils.retrieveFileSize(PATH_TO_FINAL_DOCNO+ ".txt"));

            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);

            instance.docsNo = VariableByteCompressor.decompressArray(bytes);
            docNoChannel.close();
        } catch (IOException e) {
            System.out.println("Cannot read the DocIndex");
        }

    }
}