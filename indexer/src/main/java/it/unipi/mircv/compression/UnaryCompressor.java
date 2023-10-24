package it.unipi.mircv.compression;

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
    /**
     * it returns the entire array of bytes of the compression of the array given as input
     * @param array of integer to be compressed
     * @return array of bytes of the compressed integers
     */
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
    }

    //TODO decompression



}
