package it.unipi.mircv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static it.unipi.mircv.Constants.INV_INDEX_DEBUG;
import static it.unipi.mircv.Constants.LEXICON_DEBUG;

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


}
