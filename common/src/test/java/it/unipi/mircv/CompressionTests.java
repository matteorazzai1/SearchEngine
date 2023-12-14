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
        //test compressInt
        int[] input = {5, 10, 12};
        String[] expected = {"11110", "1111111110", "111111111110"};
        for(int i = 0; i < input.length; i++){
            String output = UnaryCompressor.compressInt(input[i]);
            assertTrue(output.equals(expected[i]));
        }
        //test compressArrayInt
        int[][] input2 = {{5, 10, 12}, {1, 2, 3, 4, 5}, {6, 7, 11, 13}};
        byte[][] expected2 = {{-9, -3, -1, -64}, {91, -68}, {-5, -9, -2, -1, -16}};
        for(int i = 0; i < input2.length; i++){
            byte[] output = UnaryCompressor.compressArrayInt(input2[i]);
            for(int j = 0; j < output.length; j++){
                assertTrue(output[j] == expected2[i][j]);
            }
        }
        //test decompressArrayInt
        byte[][] input3 = {{-9, -3, -1, -64}, {91, -68}, {-5, -9, -2, -1, -16}};
        int[][] expected3 = {{5, 10, 12}, {1, 2, 3, 4, 5}, {6, 7, 11, 13}};
        int[] lengths = {3, 5, 4};
        for(int i = 0; i < input3.length; i++){
            int[] output = UnaryCompressor.decompressArrayInt(input3[i], expected3[i].length);
            for(int j = 0; j < output.length; j++){
                assertTrue(output[j] == expected3[i][j]);
            }
        }
    }

    @Test
    //test the variable byte compressor
    public void testVariableByteCompressor(){
        //test compressInt
        int[] input = {5, 312, 66000};
        byte[][] expected = {{5}, {2, -72}, {4, -125, -48}};
        for(int i = 0; i < input.length; i++){
            byte[] output = VariableByteCompressor.compressInt(input[i]);
            assertTrue(Arrays.equals(output, expected[i]));
        }
        //test decompressInt
        int[][] input2 = {{5, 312, 66000}, {129, 32770}};
        byte[][] expected2 = {{5, 2, -72, 4, -125, -48}, {1, -127, 2, -128, -126}};
        for(int i = 0; i < input2.length; i++){
            byte[] output = VariableByteCompressor.compressArrayInt(input2[i]);
            for(int j = 0; j < output.length; j++){
                assertTrue(output[j] == expected2[i][j]);
            }
        }
        //test compressArrayInt
        byte[][] input3 = {{5, 2, -72, 4, -125, -48}, {1, -127, 2, -128, -126}};
        int[][] expected3 = {{5, 312, 66000}, {129, 32770}};
        int[] lengths = {3, 2};
        for(int i = 0; i < input3.length; i++){
            int[] output = VariableByteCompressor.decompressArray(input3[i]);
            for(int j = 0; j < output.length; j++){
                assertTrue(output[j] == expected3[i][j]);
            }
        }
    }
}
