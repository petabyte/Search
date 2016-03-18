import org.apache.hadoop.io.Text;

/**
 * Created by U0155811 on 3/17/2016.
 */
public class KeywordParser {
        private String keyWord = null;

        public void parseKeyWord(Text lineRecordText){
            keyWord = null;
            String lineRecord = lineRecordText.toString();
            String [] lineSplit = lineRecord.split("\\t+");
            String firstElement = lineSplit[1];
            if( firstElement != null && firstElement.length() > 0 ){
                keyWord = firstElement.replaceAll("\\s+"," ");
                keyWord = keyWord.replaceAll("^\"\\s", "\"");
                keyWord = keyWord.replaceAll("^\"\"", "\"");
                keyWord = keyWord.replaceAll("\\s\"$","\"");
                keyWord = keyWord.replaceAll("\"\"$","\"");
                keyWord = keyWord.toLowerCase();
            }
        }

        public boolean foundKeyWord(){
            if(keyWord != null && keyWord.length() > 0){
                return true;
            }
            return false;
        }

        public String getKeyWord(){
            return keyWord;
        }
}

