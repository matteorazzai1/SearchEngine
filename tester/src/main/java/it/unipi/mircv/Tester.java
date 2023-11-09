package it.unipi.mircv;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Scanner;

import static it.unipi.mircv.SPIMI.createBuffer;
import static it.unipi.mircv.Preprocesser.process;
import static it.unipi.mircv.Constants.PATH_TO_QUERIES;
public class Tester {

    public static void main(String[] args) throws IOException {
        System.out.println("The system is starting...");
        //TODO operazioni di setup
        while(true){
            System.out.println("Select one of the following options: \n 1: Conjunctive query\n 2: Disjunctive query\n 3: exit");
            Scanner sc = new Scanner(System.in);
            String choice=sc.nextLine();
            if(Objects.equals(choice, "0"))
                break;
            else if(Objects.equals(choice, "1"))
                System.out.println("Test of conjunctive queries");
            else if(Objects.equals(choice, "2"))
                System.out.println("Test of disjunctive queries");
            else {
                System.out.println("Incorrect choice, insert a valid number");
                continue;
            }
            String query;
            String[] query_split;
            int num_query=0;
            ArrayList<Long> query_time = new ArrayList<>();
            long total_time=0;

            BufferedReader br = createBuffer(PATH_TO_QUERIES, true);
            query= br.readLine();


            while(query!=null){
                query=process(query);
                query_split=query.split("\t");
                query=query_split[1];
                long query_start = System.currentTimeMillis();
                //TODO processa la query
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