package it.unipi.mircv;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Objects;

import static it.unipi.mircv.Constants.LEXICON_PATH;
import static it.unipi.mircv.LexiconEntry.ENTRY_SIZE_LEXICON;
import static it.unipi.mircv.LexiconEntry.readLexEntryFromDisk;

public class Lexicon {

    public HashMap<String, LexiconEntry> lexicon;

    public HashMap<String, LexiconEntry> getLexicon() {
        return this.lexicon;
    }

    public void setLexicon(HashMap<String, LexiconEntry> lexicon) {
        this.lexicon = lexicon;
    }

    public void addElement(String term, LexiconEntry le){
        lexicon.put(term, le);
    }

    /**
     * costructor of lexicon reading it from disk
     */
    /*public Lexicon() throws IOException {

        this.lexicon=new HashMap<>();

        FileChannel lexiconFC=(FileChannel) Files.newByteChannel(Paths.get(LEXICON_PATH),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);

        for(int i=0;i<lexiconFC.size()-ENTRY_SIZE_LEXICON;i+= ENTRY_SIZE_LEXICON){ //TODO correct this, i put in this way because the last line of the lexicon was empty
                    //System.out.println(i);
                    LexiconEntry lexEntry=LexiconEntry.readLexEntryFromDisk(i,lexiconFC);
                    //System.out.println(lexEntry.getTerm());
                    this.lexicon.put(lexEntry.getTerm(),lexEntry);
                    //System.out.println(this.lexicon);
        }
        lexiconFC.close();
        //System.out.println("lexicon chiuso");
    }*/

    /**
     * binary search on lexicon file to find the entry of the term
     * @param term which we are searching the entry in lexicon
     * @return the lexiconEntry of the term passed to the function
     */
    public static LexiconEntry retrieveEntryFromDisk(String term) throws IOException {

        FileChannel lexiconFC=(FileChannel) Files.newByteChannel(Paths.get(LEXICON_PATH),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);

        long lexiconSize=FileUtils.retrieveFileSize();


        long startInterval=0;
        long endInterval=lexiconSize;


        while(startInterval<=endInterval) {

            long midSize = (startInterval+endInterval) / 2;


            long positionTerm = ((midSize / ENTRY_SIZE_LEXICON)) * ENTRY_SIZE_LEXICON; //it takes the quotient integer of the division, and it multiplies fo ENTRY_SIZE_LEXICON to find the starting point of the position of the term


            LexiconEntry lexEntry = LexiconEntry.readLexEntryFromDisk(positionTerm, lexiconFC);


            int comparison = term.compareTo(lexEntry.getTerm());

            if (comparison < 0) {
                //the term that we are searching is lexicographically smaller than the retrieved term
                endInterval=midSize;

            } else if (comparison > 0) {
                //the term that we are searching is lexicographically bigger than the retrieved term
                startInterval=midSize;
            } else {
                //the two terms are equal
                return lexEntry;
            }
        }
        return null;
    }

}
