package it.unipi.mircv;

import it.unipi.mircv.baseStructure.Lexicon;
import it.unipi.mircv.baseStructure.LexiconEntry;
import it.unipi.mircv.baseStructure.PostingList;
import org.junit.platform.commons.util.LruCache;

import java.io.IOException;
import java.nio.channels.FileChannel;

import static it.unipi.mircv.baseStructure.LexiconEntry.ENTRY_SIZE_LEXICON;
import static it.unipi.mircv.baseStructure.SkippingBlock.readSkippingBlocks;

public class LRUCache {
    static final int dimensionCache = 32 * 1024; //32 Kilobyte for each cache
    private final static LruCache<String, LexiconEntry> lexCacheEntries= new LruCache<>((int) (dimensionCache/ENTRY_SIZE_LEXICON));

    private final static LruCache<String, PostingList> postingCacheEntries= new LruCache<>(4);

    public static LexiconEntry retrieveLexEntry(String term) throws IOException {
        LexiconEntry lexiconEntry=lexCacheEntries.get(term);
        if(lexiconEntry==null){
            lexiconEntry=Lexicon.retrieveEntryFromDisk(term);
            lexCacheEntries.put(term,lexiconEntry); //it should remove automatically the least recently used entry
            return lexiconEntry;
        }
        return lexiconEntry;
    }

    public static PostingList retrievePostingList(LexiconEntry l, FileChannel blocksChannel) throws IOException {
        PostingList postingList=postingCacheEntries.get(l.getTerm());
        if(postingList==null){
            postingList=new PostingList(l.getTerm(), readSkippingBlocks(l.getDescriptorOffset(), blocksChannel).retrieveBlock());
            postingCacheEntries.put(l.getTerm(),postingList); //it should remove automatically the least recently used entry
            return postingList;
        }
        return postingList;
    }


    public static void clearLexCache(){
        lexCacheEntries.clear();
    }

    public static void clearPostingCache(){
        postingCacheEntries.clear();
    }

    public static LruCache<String, LexiconEntry> getLexCacheEntries() {
        return lexCacheEntries;
    }

    public static LruCache<String, PostingList> getPostingCacheEntries() {
        return postingCacheEntries;
    }

}
