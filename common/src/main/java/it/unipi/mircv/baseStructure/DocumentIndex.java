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

    private int[] docsNo;
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

    public int[] getDocsNo() {
        return docsNo;
    }

    public void setDocsNo(int[] docsNo) {
        this.docsNo = docsNo;
    }


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

    public void saveCollectionStats(Double AVDL, int collectionSize) {
        Preferences prefs = Preferences.userNodeForPackage(DocumentIndex.class);
        prefs.put("AVDL", String.valueOf(AVDL));
        prefs.put("collectionSize", String.valueOf(collectionSize));
    }

    public void loadCollectionStats() {
        Preferences prefs = Preferences.userNodeForPackage(DocumentIndex.class);
        this.setAVDL(Double.parseDouble(prefs.get("AVDL", "")));
        this.setCollectionSize(Integer.parseInt(prefs.get("collectionSize", "")));
    }

    public void retrieveDocsNo() throws IOException {

        instance.docsNo = new int[instance.getCollectionSize()];

        FileChannel docNoChannel=(FileChannel) Files.newByteChannel(Paths.get(PATH_TO_FINAL_DOCNO + ".txt"),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);
        try {

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