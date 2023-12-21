package it.unipi.mircv.compression;

import java.util.ArrayList;

public class UnaryCompressor {



    /**
     * Compress an array of integers using unary encoding
     * @param data array of integers to compress
     * @return compressed array
     */

    public static byte[] compressArrayInt(int[] data){

        int nbits = 0;

        //calculate the number of bits needed
        for (int value:data){
            nbits += value;
        }

        //retrieve the number of bytes needed
        int nBytes=(nbits+7)/8;

        //create the array of bytes
        byte[] compressedArray=new byte[nBytes];

        int j = 0; //index of the array of bytes
        int numBit = 0; //number of bits written in the current byte of the array
        int n = 0; //byte to write in the array which represents the unary encoding of the integer

        for(int i = 0; i < data.length;i++){
            if(data[i] <= 0){
                i++;
                continue;
            }

            int value = data[i]; //value to compress
            int nbitsValue = 0; //bits written for the current value

            while (nbitsValue < value-1){
                if (numBit == 8){ //if the current byte is full, write it in the array and reset the variables needed for the compression
                    compressedArray[j] = (byte) n;
                    j++;
                    numBit = 0;
                    n = 0;
                }
                n = n << 1; //shift the bits of the byte to the left
                n = n | 1; //add a 1 to the least significant bit
                numBit++;
                nbitsValue++;

            }

            if (numBit == 8){
                compressedArray[j] = (byte) n;
                j++;
                numBit = 0;
                n = 0;
            }
            n = n << 1; //to add a 0 to the least significant bit
            numBit++;
        }

        if (j < nBytes){
            n = n << (8 - numBit); //shift the bits of the byte to the left for the remaining bits to fill the byte
            compressedArray[j] = (byte) n;
        }

        return compressedArray;
    }

    /**
     * Decompress an array of bytes using unary encoding
     * @param compressedData array of byte  to decompress
     * @param numbersToRead number of integers that we have to decompress
     * @return decompressed array
     */
    public static int[] decompressArrayInt(byte[] compressedData, int numbersToRead){

        ArrayList<Integer> decompressedArray = new ArrayList<>();

        int k = 0; //number of bits read in the current byte
        int n = 1; //number of 1s read in the current byte, we start from 1 because we count only the 1s and not the 0, that we count with this 1 at the beginning
        for(int i=0;i<compressedData.length;i++){
            int value = compressedData[i]; //byte to decompress
            while (k < 8 && decompressedArray.size() < numbersToRead){
                if ((value & 128) == 128){ //if the most significant bit is 1
                    n++; //increment the number of 1s read
                }
                else{ //if the most significant bit is 0, we have read all the 1s related to the unary encoding of the integer

                    decompressedArray.add(n); //add the integer to the decompressed array

                    n = 1; //reset the number of 1s read
                }
                value = value << 1; //shift the bits of the byte to the left, to read the next bit
                k++; //increment the number of bits read
            }
            k = 0; //reset the number of bits read

        }

        return decompressedArray.stream().mapToInt(Integer::intValue).toArray();
    }
}
