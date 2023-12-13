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


    public static Long retrieveFileSize(String Path) {
        File file = new File(Path);

        long fileSizeInBytes = 0;

        if (file.exists()) {
            fileSizeInBytes = file.length();
            //System.out.println("File size in bytes: " + fileSizeInBytes);
        } else {
            //System.out.println("File does not exist");
            fileSizeInBytes= Long.parseLong(null);
        }
        return fileSizeInBytes;
    }

    public static void appendFile(String pathSource,String pathDest){

        Path sourcePath = Paths.get(pathSource);
        Path destinationPath = Paths.get(pathDest);

        try {

            byte[] data = Files.readAllBytes(sourcePath);
            Files.write(destinationPath, data, StandardOpenOption.APPEND);

            //System.out.println("Content of " + pathSource + " appended to " + pathDest + " successfully!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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

    public static void clearFolder(String pathToIntermediateIndexFolder) {
        File folder = new File(pathToIntermediateIndexFolder);
        File[] files = folder.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                f.delete();
            }
        }
    }

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
