package it.unipi.mircv;

import it.unipi.mircv.baseStructure.PostingList;
import it.unipi.mircv.baseStructure.SkippingBlock;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Scanner;

import static it.unipi.mircv.Constants.BLOCK_PATH;
import static it.unipi.mircv.Constants.INV_INDEX_DOCID;

public class QueryProcesser {
    public static void main(String[] args) throws IOException {


        System.out.println("write the query");

        Scanner sc=new Scanner(System.in);

        String query=sc.nextLine();

        if(query == null || query.isEmpty()){
                System.out.println("error inserting the query");
        }
        //TODO handle query of more than one term, preprocess it and make a cycle for to pass each single term of the query to this function
        System.out.println("go on retrieve posting");
        PostingList post= new PostingList(query,PostingList.retrievePostingList(query));


        System.out.println(post);

    }
}
