package it.unipi.mircv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

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


    public static Long retrieveFileSize() {
        File file = new File(LEXICON_PATH);

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
}
