package it.unipi.mircv;

import it.unipi.mircv.compression.UnaryCompressor;
import it.unipi.mircv.compression.VariableByteCompressor;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static junit.framework.Assert.assertTrue;

public class CompressionTests {
    @Test
    //test the unary compressor
    public void testUnaryCompressor(){
        //test compressArrayInt
        int[][] input1 = {{5, 10, 12}, {1, 2, 3, 4, 5}, {6, 7, 11, 13}}; //input arrays
        byte[][] expected1 = {{-9, -3, -1, -64}, {91, -68}, {-5, -9, -2, -1, -16}}; //expected outputs
        for(int i = 0; i < input1.length; i++){
            byte[] output = UnaryCompressor.compressArrayInt(input1[i]); //compress the input array
            for(int j = 0; j < output.length; j++){
                assertTrue(output[j] == expected1[i][j]); //check if the output is equal to the expected output
            }
        }
        //test decompressArrayInt
        byte[][] input2 = {{-9, -3, -1, -64}, {91, -68}, {-5, -9, -2, -1, -16}}; //input arrays
        int[][] expected2 = {{5, 10, 12}, {1, 2, 3, 4, 5}, {6, 7, 11, 13}}; //expected outputs
        for(int i = 0; i < input2.length; i++){
            int[] output = UnaryCompressor.decompressArrayInt(input2[i], expected2[i].length); //decompress the input array
            for(int j = 0; j < output.length; j++){
                assertTrue(output[j] == expected2[i][j]); //check if the output is equal to the expected output
            }
        }
    }

    @Test
    //test the variable byte compressor
    public void testVariableByteCompressor(){
        //test compressInt
        int[] input = {5, 312, 66000}; //input arrays
        byte[][] expected = {{5}, {2, -72}, {4, -125, -48}}; //expected outputs
        for(int i = 0; i < input.length; i++){
            byte[] output = VariableByteCompressor.integerCompression(input[i]); //compress the i-th input
            assertTrue(Arrays.equals(output, expected[i])); //check if the output is equal to the expected output
        }
        //test decompressInt
        int[][] input2 = {{5, 312, 66000}, {129, 32770}}; //input arrays
        byte[][] expected2 = {{5, 2, -72, 4, -125, -48}, {1, -127, 2, -128, -126}}; //expected outputs
        for(int i = 0; i < input2.length; i++){
            byte[] output = VariableByteCompressor.compressArrayInt(input2[i]); //compress the input array
            for(int j = 0; j < output.length; j++){
                assertTrue(output[j] == expected2[i][j]); //check if the output is equal to the expected output
            }
        }
        //test compressArrayInt
        byte[][] input3 = {{5, 2, -72, 4, -125, -48}, {1, -127, 2, -128, -126}}; //input arrays
        int[][] expected3 = {{5, 312, 66000}, {129, 32770}}; //expected outputs
        for(int i = 0; i < input3.length; i++){
            int[] output = VariableByteCompressor.decompressArray(input3[i]); //decompress the input array
            for(int j = 0; j < output.length; j++){
                assertTrue(output[j] == expected3[i][j]); //check if the output is equal to the expected output
            }
        }
    }
}
