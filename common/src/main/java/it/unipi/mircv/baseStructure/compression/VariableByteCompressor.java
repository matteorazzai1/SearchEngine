package it.unipi.mircv.baseStructure.compression;

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
                if(start <=0){
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


    /**
     *  it takes the arrayCompressed vector with byte that represents some integers in VariableByte Encoding, and it
     *  returns the integer value
     * @param arrayCompressed the array of byte to convert
     * @param numbers the number of integer to retrieve
     * @return the array of integer decompressed
     */
    public static int[] decompressArray(byte[] arrayCompressed, int numbers){

            int[] decompressedArray=new int[numbers];

            int count=0; //count bytes starting with zero
            int nextValue=0;

            StringBuilder value=new StringBuilder();

            for(int i=0;i<arrayCompressed.length;i++){

                String byteCompressed=String.format("%8s", Integer.toBinaryString(arrayCompressed[i] & 0xFF)).replace(' ', '0');

                if(byteCompressed.charAt(0)=='0'){
                    if(count==0) {
                        //it is the first byte related to the first integer
                        count++; //for understand that the next one is the initial byte for the next integer
                        value.append(byteCompressed);
                    }
                    else{
                        //the last byte was the last one for the previous integer, so we need to save that values in the array

                        decompressedArray[nextValue]=Integer.parseInt(String.valueOf(value),2);
                        nextValue++;
                        //this is the first byte for the next integer
                        value=new StringBuilder(); // to make empty the stringBuilder for the next integer
                        value.append(byteCompressed);
                    }
                }
                else{
                    //first bit of the byte is one, it has to be discarded, and we take the remainder
                    value.append(byteCompressed.substring(1));
                }
            }
            //we miss to construct the last integer of the array, that not finding another byte starting with zero, it is not been
            //constructed during the cycle for

            decompressedArray[nextValue]=Integer.parseInt(String.valueOf(value),2);

            return decompressedArray;
    }

}
