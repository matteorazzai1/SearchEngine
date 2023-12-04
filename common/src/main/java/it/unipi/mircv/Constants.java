package it.unipi.mircv;

public class Constants {
    public static final String PATH_TO_COLLECTION = "indexer/data/collection.tar.gz";
    public static final String PATH_TO_QUERIES = "tester/data/msmarco-test2020-queries.tsv";
    public static final String PATH_TO_INTERMEDIATE_INDEX = "indexer/data/IntermediateIndexes/IntermediateIndex";
    public static final String PATH_TO_INTERMEDIATE_INDEX_FOLDER = "indexer/data/IntermediateIndexes";

    public static final String PATH_TO_FINAL_DOCINDEX = "indexer/data/docIndex";
    public static final String PATH_TO_FINAL_DOCNO = "indexer/data/docNo";

    public static final String LEXICON_PATH="indexer/data/lexicon.dat";
    public static final String INV_INDEX_DEBUG="indexer/data/invIndex_debug.txt";

    public static final String LEXICON_DEBUG="indexer/data/lexicon_debug.txt";

    public static final String INV_INDEX_DOCID="indexer/data/inv_index_docId.dat";
    public static final String INV_INDEX_FREQS="indexer/data/inv_index_freq.dat";

    public static final String BLOCK_PATH="indexer/data/block_file.dat";
    public static final String BLOCK_DEBUG_PATH="indexer/data/block_debug_file.txt";
    public static final float k1 = 1.5f;
    public static final float b = 0.75f;
}
