package es.udc.fi.ri;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class SimilarDocs {
    public static void main(String[] args){
        String indexPath = "index";
        String docsID = null;
        String field = null;
        Integer top = null;
        String rep = null;

        for(int i = 0; i < args.length; i++){
            switch (args[i]){
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-doc":
                    docsID = args[++i];
                    break;
                case "-field":
                    field = args[++i];
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;
                case "-rep":
                    rep = args[++i];
                    break;
            }
        }

        if(docsID == null | field == null | top == null | rep == null){
            System.exit(1);
        }

        Directory dir = null;
        DirectoryReader indexReader = null;

        try{
            dir = FSDirectory.open(Paths.get(indexPath));
            indexReader = DirectoryReader.open(dir);

            Map<TermsEnum, Integer> map = new HashMap<>();

            Terms terms = indexReader.getTermVector(Integer.parseInt(docsID), field);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
