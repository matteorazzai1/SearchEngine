package it.unipi.mircv;

public class Posting {

        private int docId;

        private int frequency;

        public Posting(int docId,int frequency){
                this.docId=docId;
                this.frequency=frequency;
        }

        public int getDocId() {
                return docId;
        }

        public int getFrequency() {
                return frequency;
        }

        public void setDocId(int docId) {
                this.docId = docId;
        }

        public void setFrequency(int frequency) {
                this.frequency = frequency;
        }

        @Override
        public String toString() {
                return this.docId+":"+frequency;
        }
}
