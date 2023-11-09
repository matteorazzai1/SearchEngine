package it.unipi.mircv;
import java.util.*;
import java.util.stream.Collectors;

import static it.unipi.mircv.Utils.minDocID;
import static it.unipi.mircv.Utils.queryToDict;
import static java.lang.Float.POSITIVE_INFINITY;
import static java.lang.Math.log;

public class Ranking {

    //index is fine as a list, since we do not need to access a specific entry, while the lexicon entry is needed as
    //Map because we need the IDF of a specific term
    public static HashMap<Integer, Float> DAAT(LinkedList<PostingList> index, LinkedList<LexiconEntry> entries, String query){
        HashMap<String, Integer> processedQuery = queryToDict(query);
        HashMap<Integer, Float> finalScores = new HashMap<>();
        Map<String, Integer> positions = index.stream()
                .collect(Collectors.toMap(PostingList::getTerm, term -> 0));
        Map<String, Double> lexiconMap = entries.stream()
                .collect(Collectors.toMap(LexiconEntry::getTerm, LexiconEntry::getIdf));

        int minID = minDocID(index, (HashMap<String, Integer>) positions);
        float scoreAccumulator;
        Posting currentDoc;

        while(minID != (int) POSITIVE_INFINITY){
            scoreAccumulator = 0;
            for (PostingList p : index){
                if ( p.getPostings().size() > positions.get(p.getTerm())){
                    currentDoc = p.getPostings().get(positions.get(p.getTerm()));
                    if (currentDoc.getDocId() == minID){
                        scoreAccumulator += processedQuery.get(p.getTerm()) *
                                (1 + log(currentDoc.getFrequency()) * log(lexiconMap.get(p.getTerm())));
                        positions.put(p.getTerm(), positions.get(p.getTerm()) + 1);
                    }
                }
            }
            finalScores.put(minID, scoreAccumulator);
            minID = minDocID(index, (HashMap<String, Integer>) positions);
        }
        return   finalScores.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));
    }

    public static void main(String[] args){
        LinkedList<PostingList> index = new LinkedList<>();
        LinkedList<LexiconEntry> entries = new LinkedList();
        String query = "bejing duck recipe";

        PostingList p1 = new PostingList("bejing\t1:3 2:1");
        PostingList p2 = new PostingList("duck\t2:1 3:3");
        PostingList p3 = new PostingList("recipe\t1:1 3:2");

        index.add(p1);
        index.add(p2);
        index.add(p3);

        LexiconEntry l1 = new LexiconEntry("bejing");
        LexiconEntry l2 = new LexiconEntry("duck");
        LexiconEntry l3 = new LexiconEntry("recipe");

        l1.setIdf(0.65);
        l2.setIdf(0.18);
        l3.setIdf(0.61);

        entries.add(l1);
        entries.add(l2);
        entries.add(l3);


        System.out.println(DAAT(index, entries, query));


    }
}
