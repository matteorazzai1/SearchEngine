package it.unipi.mircv;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;

import static it.unipi.mircv.LexiconEntry.ENTRY_SIZE_LEXICON;

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
    public Lexicon() throws IOException {

        String lexiconPath="indexer/data/lexicon.dat";
        this.lexicon=new HashMap<>();

        FileChannel lexiconFC=(FileChannel) Files.newByteChannel(Paths.get(lexiconPath),
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
    }
}
