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


    /**
     * it takes the arrayCompressed vector with byte that represents some integers in Unary Encoding, and it
     * returns the integer value
     * @param arrayCompressed the array of byte to convert
     * @param lengthArray the number of integer to retrieve
     * @return the array of integer decompressed
     */
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
    }

    public static byte[] compressIntArray(int[] data){

        int nBits = 0;

        // Retrieve total number of bits to be written: number + 1 (zero bit separator)
        for (int num : data) {
            if (num > 0) {
                nBits += num;
            } else {
                System.out.println("Skipped element lower than 0 in the list of integers to be compressed");
            }
        }

        // Retrieve total number of bytes needed as ceil of nBits/8
        int nBytes = (nBits + 7) / 8;
        // System.out.println(nBits + " " + nBytes);

        // Initialization of array for the unary representation
        byte[] compressedArray = new byte[nBytes];

        int nextByteToWrite = 0;
        int nextBitToWrite = 0;

        // Compress each integer
        for (int num : data) {
            if (num <= 0) {
                continue;
            }

            for (int j = 0; j < num - 1; j++) {
                // Setting the j-th bit starting from left to 1
                compressedArray[nextByteToWrite] |= (byte) (1 << (7 - nextBitToWrite));

                // Update counters for next bit to write
                nextBitToWrite++;

                // Check if the current byte has been filled
                if (nextBitToWrite == 8) {
                    // New byte must be written as the next byte
                    nextByteToWrite++;
                    nextBitToWrite = 0;
                }
            }

            // Skip a bit since we should encode a 0 (which is the default value) as the last bit
            // of the Unary encoding of the integer to be compressed
            nextBitToWrite++;

            // Check if the current byte has been filled
            if (nextBitToWrite == 8) {
                // New byte must be written as the next byte
                nextByteToWrite++;
                nextBitToWrite = 0;
            }
        }

        return compressedArray;

    }

    public static int[] decompressIntArray(byte[] compressedData, int length) {

        int[] decompressedArray = new int[length];

        int toBeReadByte = 0;
        int toBeReadBit = 0;
        int nextInteger = 0;
        int onesCounter = 0;

        // Process each bit
        for(int i=0; i < compressedData.length * 8; i++){

            // Create a byte b where only the bit (i%8)-th is set
            byte b = 0b00000000;
            b |=  (1 << 7 - (i%8));

            // Check if in the byte to be read the bit (i%8)-th is set to 1 or 0
            if((compressedData[toBeReadByte] & b)==0){
                // i-th bit is set to 0

                // Writing the decompressed number in the array of the results
                decompressedArray[nextInteger] = onesCounter + 1;

                // The decompression of a new integer ends with this bit
                nextInteger++;

                if(nextInteger==length)
                    break;

                // resetting the counter of ones for next integer
                onesCounter = 0;

            } else{
                // i-th bit is set to 1

                // Increment the counter of ones
                onesCounter++;
            }

            toBeReadBit++;

            if(toBeReadBit==8){
                toBeReadByte++;
                toBeReadBit=0;
            }
        }

        return decompressedArray;
    }
}
