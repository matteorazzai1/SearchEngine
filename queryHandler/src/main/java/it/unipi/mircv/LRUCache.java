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

    //cache for lexicon entries, the key is the term and the value is the lexicon entry. The size of the cache is 32KB, so we make it contains 32KB/ENTRY_SIZE_LEXICON entries to fully exploit the cache
    private final static LruCache<String, LexiconEntry> lexCacheEntries= new LruCache<>((int) (dimensionCache/ENTRY_SIZE_LEXICON));

    //cache for posting lists, the key is the term and the value is the posting list. The size of the cache is 32KB, so we decide to put 4 posting lists in the cache seen that the maximum size of a posting list is 10KB,
    // but often the dimension of a posting list is less and 4 is a medium number of term in a query, so it seemed like a good threshold
    private final static LruCache<String, PostingList> postingCacheEntries= new LruCache<>(4);

    /**
     * This method retrieves the lexicon entry from the cache if it is present, otherwise it retrieves it from the disk
     * @param term of the lexicon entry to retrieve
     * @return the lexicon entry
     * @throws IOException
     */
    public static LexiconEntry retrieveLexEntry(String term) throws IOException {
        LexiconEntry lexiconEntry=lexCacheEntries.get(term);
        if(lexiconEntry==null){
            lexiconEntry=Lexicon.retrieveEntryFromDisk(term);
            lexCacheEntries.put(term,lexiconEntry); //it removes automatically the least recently used entry
            return lexiconEntry;
        }
        return lexiconEntry;
    }

    /**
     * This method retrieves the posting list from the cache if it is present, otherwise it retrieves it from the disk
     * @param l is the lexicon entry
     * @param blocksChannel is the channel of the file containing the skipping blocks
     * @return the posting list
     * @throws IOException
     */
    public static PostingList retrievePostingList(LexiconEntry l, FileChannel blocksChannel) throws IOException {
        PostingList postingList=postingCacheEntries.get(l.getTerm());
        if(postingList==null){
            postingList=new PostingList(l.getTerm(), readSkippingBlocks(l.getDescriptorOffset(), blocksChannel).retrieveBlock());
            postingCacheEntries.put(l.getTerm(),postingList); //it removes automatically the least recently used entry
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
