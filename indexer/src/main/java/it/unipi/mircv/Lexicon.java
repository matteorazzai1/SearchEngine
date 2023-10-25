package it.unipi.mircv;

import java.util.HashMap;

public class Lexicon {

    private HashMap<String, LexiconEntry> lexicon;

    public HashMap<String, LexiconEntry> getLexicon() {
        return lexicon;
    }

    public void setLexicon(HashMap<String, LexiconEntry> lexicon) {
        this.lexicon = lexicon;
    }

    public void addElement(String term, LexiconEntry le){
        lexicon.put(term, le);
    }
}
