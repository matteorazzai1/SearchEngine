package it.unipi.mircv;
import it.unipi.mircv.baseStructure.*;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static it.unipi.mircv.Constants.*;
import static it.unipi.mircv.FileUtils.createBuffer;
import static it.unipi.mircv.MaxScore.maxScoreQuery;
import static it.unipi.mircv.Preprocesser.*;
import static it.unipi.mircv.Ranking.DAATDisjunctive;
import static it.unipi.mircv.Ranking.DAATConjunctive;

public class Tester {

    private static final Scanner scanner = new Scanner(System.in);
    private static BufferedReader br = null;
    private static FileWriter evalChannel = null;

    public static void main(String[] args) throws IOException {
        System.out.println("The system is starting...");

        DocumentIndex docIndex = DocumentIndex.getInstance();
        docIndex.loadCollectionStats();
        docIndex.readFromFile();
        boolean isEvaluation;
        String query;
        String queryType;
        String disjunctiveQueryType = null;
        boolean isBM25;
        int evaluation;
        int numResults = 0;

        while (true) {
            System.out.println("Welcome to the search engine!\n1. Free query\n2. Evaluate search engine");
            evaluation = Integer.parseInt(scanner.nextLine());
            isEvaluation = evaluation == 2;

            if (isEvaluation) {
                evalChannel = setupEvaluation();
                numResults = 100;
                docIndex.retrieveDocsNo();
                try {
                    br = createBuffer((PATH_TO_QUERIES), false);
                    query = br.readLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                System.out.println("Insert a query: ");
                query = scanner.nextLine().trim();
                System.out.println("Select how many results you want to retrieve: ");
                numResults = Integer.parseInt(scanner.nextLine().trim());
            }

            System.out.println("Select query type:\n1. Conjunctive query\n2. Disjunctive query");
            queryType = scanner.nextLine().trim();

            if(queryType.equals("2")){
                System.out.println("Select the algorithm to run:\n1. DAAT\n2. MaxScore");
                disjunctiveQueryType = scanner.nextLine().trim();
            }

            System.out.println("Select ranking type:\n1. TFIDF\n2. BM25");
            isBM25 = Integer.parseInt(scanner.nextLine().trim()) == 2;

            processQueries(query, queryType, disjunctiveQueryType, isBM25, isEvaluation, numResults);

        }
    }


    private static FileWriter setupEvaluation() throws IOException {
        if (!Files.exists(Path.of(PATH_TO_EVALUATION_RESULTS_FOLDER))) {
            Files.createDirectory(Path.of(PATH_TO_EVALUATION_RESULTS_FOLDER));
        } else {
            FileUtils.clearFolder(PATH_TO_EVALUATION_RESULTS_FOLDER);
        }
        return new FileWriter(PATH_TO_EVALUATION_RESULTS);
    }


    private static void processQueries(String query, String queryType, String disjunctiveQueryType, boolean isBM25, boolean isEvaluation, int numResults) throws IOException {
        List<Long> queryTimes = new ArrayList<>();
        long totalTime = 0;
        List<Map.Entry<Integer, Double>> results;

        while (query != null && !query.isEmpty()) {
            long queryStart = System.currentTimeMillis();
            if(isEvaluation){
                results = executeQuery(process(query).split("\t")[1] , queryType, disjunctiveQueryType, isBM25, numResults);
            }
            else {
                results = executeQuery(processCLIQuery(query) , queryType, disjunctiveQueryType, isBM25, numResults);
            }

            long queryEnd = System.currentTimeMillis();

            if (isEvaluation) {
                System.out.println("Processed query: " + query);
                saveTrecEvalResults(query, results);
                queryTimes.add(queryEnd - queryStart);
                query = br.readLine();
            } else {
                System.out.println("\n" + results);
                System.out.println("Time to execute the query: " + (queryEnd - queryStart) + " milliseconds\n");
                System.out.println("Insert a new query or press enter to go back to the starting menu");
                query = scanner.nextLine();
            }
        }

        for (Long time : queryTimes) {
            totalTime += time;
        }

        if (isEvaluation) {
            System.out.println(queryTimes.size() + " queries executed in " + totalTime / 1000 + " seconds");
            System.out.println("Average query processing time: " + (((float) totalTime / queryTimes.size())) / 1000 + " seconds");
        }

        br.close();
        evalChannel.close();
    }

    private static List<Map.Entry<Integer, Double>> executeQuery(String query, String queryType, String disjunctiveQueryType, boolean isBM25, int numResults) throws IOException {
        switch (queryType) {
            case "1":
                return DAATConjunctive(query, numResults, isBM25);
            case "2":
                if (disjunctiveQueryType.equals("1"))
                    return DAATDisjunctive(query, numResults, isBM25);
                else
                    return maxScoreQuery(query, numResults, isBM25);
            default:
                return Collections.emptyList();
        }
    }

    //TODO convert the docid to the docno
    private static void saveTrecEvalResults(String s, List<Map.Entry<Integer, Double>> results) throws IOException {
        int queryId = Integer.parseInt(s.split("\t")[0]);
        int[] docNos = DocumentIndex.getInstance().getDocsNo();
        for (int i=0; i<results.size(); i++) {
            StringBuilder sBuilder = new StringBuilder(queryId + " Q0 ");
            Map.Entry<Integer, Double> entry = results.get(i);
            sBuilder.append("D").append(docNos[entry.getKey()-1]).append(" ").append(i+1).append(" ").append(entry.getValue()).append(" RUN_EVAL\n");
            evalChannel.write(sBuilder.toString());
        }
    }
    }

