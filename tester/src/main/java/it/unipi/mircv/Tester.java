package it.unipi.mircv;
import it.unipi.mircv.baseStructure.DocumentIndex;
import it.unipi.mircv.baseStructure.Lexicon;
import it.unipi.mircv.baseStructure.LexiconEntry;
import it.unipi.mircv.baseStructure.PostingList;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

import static it.unipi.mircv.SPIMI.createBuffer;
import static it.unipi.mircv.Preprocesser.process;
import static it.unipi.mircv.Constants.PATH_TO_QUERIES;
import static it.unipi.mircv.Ranking.DAAT;
public class Tester {

    public static void main(String[] args) throws IOException {
        System.out.println("The system is starting...");
        String query_choice;
        String disjunctive_type = "";
        String ranking_type;

        LinkedList<LexiconEntry> entries=new LinkedList<>();
        LinkedList<PostingList> index = new LinkedList<>();
        //TODO operazioni di setup
        while(true){
            System.out.println("Select one of the following options: \n 1: Conjunctive query\n 2: Disjunctive query\n 3: exit");
            Scanner sc = new Scanner(System.in);
            query_choice=sc.nextLine();
            if(Objects.equals(query_choice, "0"))
                break;
            else if(Objects.equals(query_choice, "1"))
                System.out.println("Test of conjunctive queries");
            else if(Objects.equals(query_choice, "2")) {
                System.out.println("Test of disjunctive queries");
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
            String query;
            String[] query_split;
            int num_query=0;
            ArrayList<Long> query_time = new ArrayList<>();
            long total_time=0;

            BufferedReader br = createBuffer(PATH_TO_QUERIES, false);
            query= br.readLine();


            while(query!=null){
                query=process(query);
                query_split=query.split("\t");
                query=query_split[1];
                System.out.println("query: "+query);
                for(String term : query.split(" ")){
                    System.out.println("term: "+term);
                    PostingList post = new PostingList(term, PostingList.retrievePostingList(term));
                    index.add(post);
                    entries.add(Lexicon.retrieveEntryFromDisk(term));
                }
                long query_start = 0;
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
                            DAAT(index, entries , query,false);
                            //TODO execute disjunctive with DAAT and TFIDF
                        }else{//BM25
                            query_start=System.currentTimeMillis();
                            DAAT(index, entries , query,true);
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
                long query_end = System.currentTimeMillis();

                query_time.add(query_end-query_start);
                num_query++;
                query=br.readLine();
            }

            for(Long time : query_time){
                total_time+=time;
            }

            System.out.println(num_query+" queries executed in "+total_time/1000+" seconds");
            System.out.println("average query processing time: "+(total_time/num_query)/1000+" seconds");
        }
    }
}