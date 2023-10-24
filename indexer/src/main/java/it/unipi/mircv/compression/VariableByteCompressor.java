package it.unipi.mircv.compression;

import java.util.ArrayList;

import static java.lang.Math.log;

public class VariableByteCompressor {

    /**
     * it returns the variable-byte codes of the integer given as input
     * @param value is the integer to compress
     * @return the variable-byte code
     */
    public static byte[] compressInt(int value){
            String binaryValue=Integer.toBinaryString(value);

            //we divide by 7 to search the bytes of the representation of the value in Variable-Byte Code
            int nBytes=(binaryValue.length())/7;

            if((binaryValue.length())%7!=0){
                    nBytes++; //if there is a remainder, it means that we need another bytes for the remaining bits
            }

            byte[] compressedValue=new byte[nBytes];

            for(int i=0;i<nBytes;i++){
                int end=(binaryValue.length() - (7 * i));
                int start = end - 7;
                StringBuilder byteSubstring=new StringBuilder();
                if(start <0){
                    start=0;
                    byteSubstring.append("0".repeat(Math.max(0, (8 - binaryValue.substring(start, end).length())))); //equivalent of a for that put many zeros how many are the bit left to arrive to 8
                    byteSubstring.append(binaryValue, start, end); //substring from start to end of the string binaryValue
                    compressedValue[(nBytes - 1 - i)] = (byte) Integer.parseInt(String.valueOf(byteSubstring), 2);
                }
                else {
                    byteSubstring.append('1');
                    byteSubstring.append(binaryValue, start, end);  //substring from start to end of the string binaryValue
                    compressedValue[(nBytes - 1 - i)] = (byte) Integer.parseInt(String.valueOf(byteSubstring), 2);
                }

            }
            return compressedValue;
        }

    /**
     * it returns the entire array of bytes of the compression of the array given as input
     * @param array of integer to be compressed
     * @return array of bytes of the compressed integers
     */
        public static byte[] compressArrayInt(int[] array){

            ArrayList<Byte> compressedArray=new ArrayList<>();

            for(int value:array){
                for(Byte compressByte:compressInt(value))
                    compressedArray.add(compressByte);
            }

            byte[] compressedArrayByte=new byte[compressedArray.size()];
            for(int i=0;i<compressedArray.size();i++){
                    compressedArrayByte[i]=compressedArray.get(i);
            }
            return compressedArrayByte;
        }

    //TODO decompression

}
