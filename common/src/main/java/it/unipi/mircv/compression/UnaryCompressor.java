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
        for (int value:data){
            nbits += value;
        }
        int nBytes=(nbits+7)/8;
        byte[] compressedArray=new byte[nBytes];
        int j = 0;
        int numBit = 0;
        int n = 0;
        for(int i = 0; i < data.length;i++){
            if(data[i] <= 0){
                i++;
                continue;
            }

            int value = data[i];
            int nbitsValue = 0;

            while (nbitsValue < value-1){
                if (numBit == 8){
                    compressedArray[j] = (byte) n;
                    j++;
                    numBit = 0;
                    n = 0;
                }
                n = n << 1;
                n = n | 1;
                numBit++;
                nbitsValue++;

            }

            if (numBit == 8){
                compressedArray[j] = (byte) n;
                j++;
                numBit = 0;
                n = 0;
            }
            n = n << 1;
            numBit++;
        }

        if (j < nBytes){
            n = n << (8 - numBit);
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

        int k = 0;
        int n = 1;
        for(int i=0;i<compressedData.length;i++){
            int value = compressedData[i];
            while (k < 8 && decompressedArray.size() < numbersToRead){
                if ((value & 128) == 128){
                    n++;
                }
                else{

                    decompressedArray.add(n);

                    n = 1;
                }
                value = value << 1;
                k++;
            }
            k = 0;

        }

        return decompressedArray.stream().mapToInt(Integer::intValue).toArray();
    }
}
