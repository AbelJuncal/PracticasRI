package es.udc.fi.ri;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class BestTermsInColl {
    public static void main(String[] args){
        String indexPath = "index";
        String field = null;
        Integer top = null;
        String order = null;
        boolean rev = false;

        for(int i = 0; i < args.length; i++){
            switch (args[i]){
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-field":
                    field = args[++i];
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;
                case "-rev":
                    rev = true;
                    break;
            }
        }

        if(rev){
            order = "df";
        }else{
            order = "idf";
        }

        Directory dir = null;
        DirectoryReader indexReader = null;



        try {
            dir = FSDirectory.open(Paths.get(indexPath));
            indexReader = DirectoryReader.open(dir);

            Map<String, Integer> map = new HashMap<>();

            Terms terms = MultiTerms.getTerms(indexReader, field);

            TermsEnum iterate = terms.iterator();

            while (iterate.next() != null) {
                map.put(iterate.term().utf8ToString(), BestTermsInDoc.getValue(indexReader, iterate, order));
            }

            Stream<Map.Entry<String, Integer>> sorted = map.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue().reversed());

            System.out.println(Arrays.toString(sorted.toArray()));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
