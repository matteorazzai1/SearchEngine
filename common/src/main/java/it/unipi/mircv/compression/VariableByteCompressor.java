package it.unipi.mircv.compression;

import java.util.ArrayList;

public class VariableByteCompressor {


    /**
     * Compress an integer using variable byte encoding, function used in the compressArrayInt function to compress each integer of the array
     * @param number integer to compress
     * @return compressed array
     */

    public static byte[] integerCompression(int number){
            if (number == 0) {
                return new byte[]{0};
            }

            ArrayList<Byte> byteList = new ArrayList<>();

            while (true) {
                byteList.add((byte) ((number % 128)));
                if (number < 128) {
                    break;
                }
                number /= 128;
            }
            //byteList.set(byteList.size()-1, (byte) (byteList.get(0) - 128));

            // Reverse the list
            byte[] reversedList = new byte[byteList.size()];
            reversedList[0]= (byte) number;
            for (int i = 0; i <byteList.size()-1; i++) {
                reversedList[byteList.size()-1-i] = (byte) ((byteList.get(i))+128);
            }

            return reversedList;

    }

    /**
     * Compress an array of integers using variable byte encoding
     * @param array array of integers to compress
     * @return compressed array
     */
    public static byte[] compressArrayInt(int[] array)  {
        ArrayList<Byte> compressedArray = new ArrayList<>();

        // For each element to be compressed
        for(int number: array){
            // Perform the compression and append the compressed output to the byte list
            for(byte elem: integerCompression(number)) {
                compressedArray.add(elem);
            }
        }
        // Convert the arraylist to an array
        byte[] output = new byte[compressedArray.size()];
        for(int i = 0; i < compressedArray.size(); i++)
            output[i] = compressedArray.get(i);

        return output;
    }
    /**
     * Decompress an array of bytes using variable byte encoding
     * @param compressedData array of bytes to decompress
     * @return decompressed array
     */

    public static int[] decompressArray(byte[] compressedData){

        ArrayList<Integer> decompressedArray = new ArrayList<>();

        int i = 0;
        int number = 0;

        while (i<compressedData.length) {
            if (compressedData[i] >= 0) {
                if(i>0){
                    decompressedArray.add(number);
                    number = 0;
                }
                number = 128 * number + compressedData[i];

            } else {
                number = 128 * number + (compressedData[i] + 128);
            }
            i++;
        }
        decompressedArray.add(number); //the last values is not been added in the cycle for, so we add it here

        return decompressedArray.stream()
                .mapToInt(Integer::intValue)
                .toArray();
    }

}
