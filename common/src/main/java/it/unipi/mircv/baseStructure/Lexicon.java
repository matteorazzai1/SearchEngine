package it.unipi.mircv.baseStructure;

import it.unipi.mircv.FileUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;

import static it.unipi.mircv.Constants.LEXICON_PATH;
import static it.unipi.mircv.baseStructure.LexiconEntry.ENTRY_SIZE_LEXICON;

public class Lexicon {

    //The collection of LexiconEntries
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
     * Binary search on lexicon file to find the entry of the term
     * @param term which we are searching the entry in lexicon
     * @return the lexiconEntry of the term passed to the function
     */
    public static LexiconEntry retrieveEntryFromDisk(String term) throws IOException {

        FileChannel lexiconFC=(FileChannel) Files.newByteChannel(Paths.get(LEXICON_PATH),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);

        long lexiconSize= FileUtils.retrieveFileSize(LEXICON_PATH);


        long startInterval=0;
        long endInterval=lexiconSize, midSize, positionTerm;
        LexiconEntry lexEntry;


        //check if the term is the first or the last of the lexicon, so we avoid performing the cycle
        LexiconEntry firstLexEntry=LexiconEntry.readLexEntryFromDisk(startInterval, lexiconFC);
        LexiconEntry lastLexEntry=LexiconEntry.readLexEntryFromDisk(endInterval-ENTRY_SIZE_LEXICON, lexiconFC);
        if(firstLexEntry.getTerm().compareTo(term)==0){
            lexiconFC.close();
            return firstLexEntry;
        }
        else if (lastLexEntry.getTerm().compareTo(term)==0){
            lexiconFC.close();
            return lastLexEntry;
        }


        while(startInterval<=endInterval) {

            midSize = (startInterval+endInterval) / 2;
            if (midSize == startInterval){
                return null;
            }

            //it takes the quotient integer of the division, and it multiplies fo ENTRY_SIZE_LEXICON
            // to find the starting point of the position of the term
            positionTerm = ((midSize / ENTRY_SIZE_LEXICON)) * ENTRY_SIZE_LEXICON;


            lexEntry = LexiconEntry.readLexEntryFromDisk(positionTerm, lexiconFC);
            int comparison = term.compareTo(lexEntry.getTerm());

            if (comparison < 0) {
                //the term that we are searching is lexicographically smaller than the retrieved term
                endInterval=midSize;

            } else if (comparison > 0) {
                //the term that we are searching is lexicographically bigger than the retrieved term
                startInterval=midSize;
            } else {
                //the two terms are equal
                lexiconFC.close();
                return lexEntry;
            }
        }
        lexiconFC.close();
        return null;
    }

}
