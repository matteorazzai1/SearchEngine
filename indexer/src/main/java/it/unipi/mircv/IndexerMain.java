package it.unipi.mircv;

import java.io.IOException;

public class IndexerMain {
    public static void main(String[] args) throws IOException {
        SPIMI.performSpimi(true);
        Merger.performMerging(false);
    }
}
