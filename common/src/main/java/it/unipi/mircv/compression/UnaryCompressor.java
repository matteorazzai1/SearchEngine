package it.unipi.mircv.compression;

import java.util.ArrayList;

public class UnaryCompressor {

    /**
     * it returns the unary codes of the integer given as input
     * @param value is the integer to compress
     * @return the unary code
     */
    public static String compressInt(int value){

        StringBuilder unaryCode=new StringBuilder();

        for(int i=0;i<value-1;i++){
            unaryCode.append('1');
        }
        unaryCode.append('0');

        return unaryCode.toString();
    }

    /*
    /**
     * it returns the entire array of bytes of the compression of the array given as input
     * @param array of integer to be compressed
     * @return array of bytes of the compressed integers
     */
/*

    public static byte[] compressArrayInt(int[] array) {

        int nbits = 0;

        StringBuilder compressedPosting = new StringBuilder();

        for (int value:array){
                compressedPosting.append(compressInt(value));
                nbits += value; //es: int(5)=unaryCode(11110), so it takes 5 bits
        }

        int nBytes=(nbits+7)/8;

        byte[] compressedArray=new byte[nBytes];

        for(int i=0;i<nBytes;i++){

            int end=0;
            if(((i*8)+8)>compressedPosting.length()){ //it goes after the end of the binaryString, so it is the last bytes, and we have to add zeros at the end to fulfill the byte
                    end=compressedPosting.length();
                    StringBuilder substring=new StringBuilder();
                    substring.append(compressedPosting.substring(i*8,end));
                    for(int j=0;j<(8-compressedPosting.substring(i*8,end).length());j++){
                            substring.append('0');
                    }
                    compressedArray[i]= (byte) Integer.parseInt(String.valueOf(substring),2);
            }
            else{
                end=(i*8)+8;
                compressedArray[i]= (byte) Integer.parseInt(compressedPosting.substring(i*8,end),2);
            }

        }

        return compressedArray;
    }*/

/*
    /**
     * it takes the arrayCompressed vector with byte that represents some integers in Unary Encoding, and it
     * returns the integer value
     * @param arrayCompressed the array of byte to convert
     * @param lengthArray the number of integer to retrieve
     * @return the array of integer decompressed
     */

/*
    public static int[] decompressArrayInt(byte[] arrayCompressed,int lengthArray){

        int[] decompressedArray=new int[lengthArray];

        StringBuilder compressedArray=new StringBuilder();

        for(int i=0;i<arrayCompressed.length;i++){

            String byteCompressed=String.format("%8s", Integer.toBinaryString(arrayCompressed[i] & 0xFF)).replace(' ', '0');
            compressedArray.append(byteCompressed);

        }

        int count=0;
        int nextValue=0;

        for (int i = 0; i < compressedArray.length(); i++) {
                if (compressedArray.charAt(i) == '1') {
                    count++;
                } else {
                    decompressedArray[nextValue] = count+1;
                    nextValue++;
                    count=0;
                }
                if(nextValue==lengthArray)
                    break;
        }

        return decompressedArray;
    }*/


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

    public static int[] decompressArrayInt(byte[] compressedData, int numbersToRead){
        //int[] decompressedArray = new int[compressedData.length * 8];
        ArrayList<Integer> decompressedArray = new ArrayList<>();
        //int j = 0;
        int k = 0;
        int n = 1;
        for(int i=0;i<compressedData.length;i++){
            int value = compressedData[i];
            while (k < 8 && decompressedArray.size() < numbersToRead){
                if ((value & 128) == 128){
                    n++;
                }
                else{
                    //decompressedArray[j] = n;
                    decompressedArray.add(n);
                    //j++;
                    n = 1;
                }
                value = value << 1;
                k++;
            }
            k = 0;
            //i++;
        }
        //if(n != 0){
            //decompressedArray.add(n);
        //}
        return decompressedArray.stream().mapToInt(Integer::intValue).toArray();
    }
}
