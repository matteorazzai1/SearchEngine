package it.unipi.mircv;

import it.unipi.mircv.baseStructure.Lexicon;
import it.unipi.mircv.baseStructure.LexiconEntry;
import it.unipi.mircv.baseStructure.PostingList;

import java.io.IOException;
import java.util.*;

import static it.unipi.mircv.Ranking.DAAT;

public class MainCli {
    public static void main(String[] args) throws IOException {
        String query_choice;
        String disjunctive_type = "";
        String ranking_type;
        LinkedList<LexiconEntry> entries=new LinkedList<>();
        LinkedList<PostingList> index = new LinkedList<>();
        System.out.println("Welcome to our search engine!"+'\n'+"Please, select a type of query:");
        while(true){
            System.out.println("1: Conjunctive query\n 2: Disjunctive query\n 3: exit");
            Scanner sc = new Scanner(System.in);
            query_choice=sc.nextLine();
            if(Objects.equals(query_choice, "3")) {
                System.out.println("Goodbye!");
                break;
            }
            else if(Objects.equals(query_choice, "1"))
                System.out.println("You select a conjunctive query");
            else if(Objects.equals(query_choice, "2")) {
                System.out.println("You select a disjunctive queries");
                System.out.println("Select one of these options: \n1) execute DAAT \n2) execute MaxScore");
                do {
                    sc = new Scanner(System.in);
                    disjunctive_type = sc.nextLine();
                }while (!disjunctive_type.equals("1") && !disjunctive_type.equals("2"));
                if(disjunctive_type.equals("1"))
                    System.out.println("You selected DAAT");
                else
                    System.out.println("You selected MaxScore");
            }
            else {
                System.out.println("Incorrect choice, insert a valid number");
                continue;
            }
            System.out.println("Select one of these metrics: \n1) TFIDF \n2) BM25");
            do {
                sc = new Scanner(System.in);
                ranking_type = sc.nextLine();
            }while (!ranking_type.equals("1") && !ranking_type.equals("2"));
            if(ranking_type.equals("1"))
                System.out.println("You selected TFIDF");
            else
                System.out.println("You selected BM25");
            System.out.println("Insert your query:");
            String query = sc.nextLine();
            while (query.isBlank()) {
                System.out.println("Insert a valid query:");
                query = sc.nextLine();
            }
            for(String term : query.split(" ")){
                PostingList pl = new PostingList(term, PostingList.retrievePostingList(term));
                index.add(pl);
                entries.add(Lexicon.retrieveEntryFromDisk(term));
            }
            long query_start = 0;
            PriorityQueue<Map.Entry<Integer, Double>> results = null;
            if(query_choice.equals("1")){//Conjunctive
                if(ranking_type.equals("1")){//TFIDF
                    query_start=System.currentTimeMillis();
                    //TODO execute conjunctive with TFIDF
                }else{//BM25
                    query_start=System.currentTimeMillis();
                    //TODO execute conjunctive with BM25
                }
            }else{//Disjunctive
                if(disjunctive_type.equals("1")){//DAAT
                    if(ranking_type.equals("1")){//TFIDF
                        query_start=System.currentTimeMillis();
                        results=DAAT(index, entries , query,false);
                        //TODO execute disjunctive with DAAT and TFIDF
                    }else{//BM25
                        query_start=System.currentTimeMillis();
                        results=DAAT(index, entries , query,true);
                        //TODO execute conjunctive with DAAT and BM25
                    }
                }else{//MaxScore
                    if(ranking_type.equals("1")){//TFIDF
                        query_start=System.currentTimeMillis();
                        //TODO execute disjunctive with MaxScore and TFIDF
                    }else{//BM25
                        query_start=System.currentTimeMillis();
                        //TODO execute conjunctive with MaxScore and BM25
                    }
                }
            }
            long query_end=System.currentTimeMillis();
            long query_time=query_end-query_start;
            if(results==null) {
                System.out.println("I am sorry, no results found");
            }
            else {
                System.out.println("Results:");
                for (Map.Entry<Integer, Double> entry : results) {
                    System.out.println("DocID: " + entry.getKey() + " Score: " + entry.getValue());
                }
            }
            System.out.println("Query time: " + query_time + " ms");
        }
    }
}
