package it.unipi.mircv;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static it.unipi.mircv.Constants.*;

public class FileUtils {

    /**
     * function to clear the debug files
     */
    public static void clearDebugFiles(){
        try {
            File file = new File(INV_INDEX_DEBUG);
            FileWriter fileWriter = new FileWriter(file,false);
            BufferedWriter writer = new BufferedWriter(fileWriter);


            writer.close(); // Close the writer to save changes

        } catch (IOException e) {
            e.printStackTrace();
        }



        try {
            File file = new File(LEXICON_DEBUG);
            FileWriter fileWriter = new FileWriter(file,false);
            BufferedWriter writer = new BufferedWriter(fileWriter);


            writer.close(); // Close the writer to save changes

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * function to retrieve the size of a file
     * @param Path of the file to retrieve the size
     * @return the size of the file
     */
    public static Long retrieveFileSize(String Path) {
        File file = new File(Path);

        long fileSizeInBytes = 0;

        if (file.exists()) {
            //file exists
            fileSizeInBytes = file.length();
        } else {
            //file does not exist
            fileSizeInBytes= Long.parseLong(null);
        }
        return fileSizeInBytes;
    }


    /**
     * function to clear a file
     * @param path of the file to clear
     */
    public static void clearFile(String path){
        try {
            File file = new File(path);
            FileWriter fileWriter = new FileWriter(file,false);
            BufferedWriter writer = new BufferedWriter(fileWriter);


            writer.close(); // Close the writer to save changes

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * function to clear a folder
     * @param pathToIntermediateIndexFolder of the folder to clear
     */
    public static void clearFolder(String pathToIntermediateIndexFolder) {
        File folder = new File(pathToIntermediateIndexFolder);
        File[] files = folder.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                f.delete();
            }
        }
    }

    /**
     * function to create a buffer to read a file, in case it is compressed or not
     * @param path of the file to create the buffer
     * @param isCompressed boolean to check if the file is compressed
     * @return the buffer
     * @throws IOException
     */
    public static BufferedReader createBuffer(String path, boolean isCompressed) throws IOException {
        if (isCompressed) {
            TarArchiveInputStream tarInput = new TarArchiveInputStream
                    (new GzipCompressorInputStream(new FileInputStream(path)));
            tarInput.getNextTarEntry();
            return new BufferedReader(new InputStreamReader(tarInput, StandardCharsets.UTF_8));
        }
        return Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8);

    }
}
