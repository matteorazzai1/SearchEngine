package it.unipi.mircv;
import it.unipi.mircv.baseStructure.*;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static it.unipi.mircv.Constants.*;
import static it.unipi.mircv.FileUtils.createBuffer;
import static it.unipi.mircv.MaxScore.maxScoreQuery;
import static it.unipi.mircv.Preprocesser.*;
import static it.unipi.mircv.Ranking.DAATDisjunctive;
import static it.unipi.mircv.Ranking.DAATConjunctive;

public class Tester {

    //scanner used to read from the command line
    private static final Scanner scanner = new Scanner(System.in);

    //reader to read the queries from the file
    private static BufferedReader br = null;

    //file to store the results of the evaluation
    private static FileWriter evalChannel = null;

    /**
     * Entry point for the project, from which user can insert queries or evaluate the search engine
     * @param args arguments to the main, actually never used
     * @throws IOException if the file of the queries is not found
     */
    public static void main(String[] args) throws IOException {
        System.out.println("The system is starting...");

        //load the collection statistics and create the variables
        DocumentIndex docIndex = DocumentIndex.getInstance();
        docIndex.loadCollectionStats();
        docIndex.readFromFile();
        docIndex.retrieveDocsNo();
        boolean isEvaluation;
        String query;
        String queryType;
        String disjunctiveQueryType = null;
        boolean isBM25;
        int evaluation;
        int numResults;

        while (true) {
            System.out.println("Welcome to the search engine!\n1. Free query\n2. Evaluate search engine\n3. Exit");
            evaluation = Integer.parseInt(scanner.nextLine());
            if (evaluation == 3) {
                System.out.println("Bye!");
                break;
            }
            isEvaluation = evaluation == 2;

            //if the user wants to evaluate the search engine, we create the file to store the results and read the queries
            if (isEvaluation) {
                evalChannel = setupEvaluation();
                numResults = 100;
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


    /**
     * Creates the file to store the results of the evaluation and the relative folder if it does not exist.
     * If the folder already exists, it is cleared.
     * @return the FileWriter to write the results
     * @throws IOException if the folder cannot be created
     */
    private static FileWriter setupEvaluation() throws IOException {
        if (!Files.exists(Path.of(PATH_TO_EVALUATION_RESULTS_FOLDER))) {
            Files.createDirectory(Path.of(PATH_TO_EVALUATION_RESULTS_FOLDER));
        } else {
            FileUtils.clearFolder(PATH_TO_EVALUATION_RESULTS_FOLDER);
        }
        return new FileWriter(PATH_TO_EVALUATION_RESULTS);
    }

    /**
     * Function to process the queries, either from the file or from the command line
     * @param query the query to process
     * @param queryType the type of the query (conjunctive or disjunctive)
     * @param disjunctiveQueryType the type of the disjunctive query (DAAT or MaxScore)
     * @param isBM25 boolean to specify if we want to use BM25 or tfidf
     * @param isEvaluation boolean to specify if we want to evaluate the search engine
     * @param numResults number of results to retrieve
     * @throws IOException if the file of the queries is not found
     */
    private static void processQueries(String query, String queryType, String disjunctiveQueryType, boolean isBM25, boolean isEvaluation, int numResults) throws IOException {
        List<Long> queryTimes = new ArrayList<>();
        long totalTime = 0;
        List<Map.Entry<Integer, Double>> results;
        String processedQuery = null;

        //either we get to the EOF of the queries or the user inserts and empty query
        while (query != null && !query.isEmpty()) {
            long queryStart = System.currentTimeMillis();
            if(isEvaluation){
                processedQuery = process(query);
                results = executeQuery(processedQuery.split("\t")[1] , queryType, disjunctiveQueryType, isBM25, numResults);
            }
            else {
                results = executeQuery(processCLIQuery(query) , queryType, disjunctiveQueryType, isBM25, numResults);
                //we map the docNo to the docID
                results = results.parallelStream()
                        .map(entry -> new AbstractMap.SimpleEntry<>(
                                DocumentIndex.getInstance().getDocsNo()[entry.getKey() - 1],
                                entry.getValue()))
                        .collect(Collectors.toList());
            }

            long queryEnd = System.currentTimeMillis();

            if (isEvaluation) {
                System.out.println("Processed query: " + query);
                //we save the results in the file
                saveTrecEvalResults(Integer.parseInt(processedQuery.split("\t")[0]), results);
                queryTimes.add(queryEnd - queryStart);
                query = br.readLine();
            } else {
                System.out.println("\n" + results);
                System.out.println("Time to execute the query: " + (queryEnd - queryStart) + " milliseconds\n");
                System.out.println("Insert a new query or press enter to go back to the starting menu");
                query = scanner.nextLine();
            }
        }

        //computing the total time for the queries from the cumulated ones
        for (Long time : queryTimes) {
            totalTime += time;
        }

        if (isEvaluation) {
            System.out.println(queryTimes.size() + " queries executed in " + totalTime / 1000 + " seconds");
            System.out.println("Average query processing time: " + (((float) totalTime / queryTimes.size())) / 1000 + " seconds");
            br.close();
            evalChannel.close();
        }


    }

    /**
     * Function to execute the query, either conjunctive or disjunctive
     * @param query the query to process
     * @param queryType the type of the query (conjunctive or disjunctive)
     * @param disjunctiveQueryType the type of the disjunctive query (DAAT or MaxScore)
     * @param isBM25 boolean to specify if we want to use BM25 or tfidf
     * @param numResults number of results to retrieve
     * @return the list of the results
     * @throws IOException if we cannot open any file
     */
    private static List<Map.Entry<Integer, Double>> executeQuery(String query, String queryType, String disjunctiveQueryType, boolean isBM25, int numResults) throws IOException {
        switch (queryType) {
            //conjunctive query
            case "1":
                return DAATConjunctive(query, numResults, isBM25);
            //disjunctive query
            case "2":
                if (disjunctiveQueryType.equals("1"))
                    //DAAT
                    return DAATDisjunctive(query, numResults, isBM25);
                else
                    //MaxScore
                    return maxScoreQuery(query, numResults, isBM25);
            default:
                return Collections.emptyList();
        }
    }

    /**
     * Function to save the results of the evaluation in the file
     * @param queryId the result to be flushed to file
     * @param results the list of the results
     * @throws IOException if we cannot write to the file
     */
    private static void saveTrecEvalResults(int queryId, List<Map.Entry<Integer, Double>> results) throws IOException {
        int[] docNos = DocumentIndex.getInstance().getDocsNo();
        for (int i=0; i<results.size(); i++) {
            StringBuilder sBuilder = new StringBuilder(queryId + " Q0 ");
            Map.Entry<Integer, Double> entry = results.get(i);
            sBuilder.append(docNos[entry.getKey()-1]).append(" ").append(i+1).append(" ").append(entry.getValue()).append(" STANDARD\n");
            evalChannel.write(sBuilder.toString());
        }
    }
    }

