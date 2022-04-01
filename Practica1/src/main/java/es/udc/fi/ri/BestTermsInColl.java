package es.udc.fi.ri;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class BestTermsInColl {
    public static void main(String[] args){
        String indexPath = null;
        String field = null;
        Integer top = null;
        String order = null;
        boolean rev = false;
        String usage = "java es.udc.fi.ri.SimilarDocs"
                + " [-index INDEX_PATH] [-field] [-top] [-rev df, idf]\n\n";

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

        if(indexPath == null || field == null || top == null){
            System.exit(1);
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

            Map<String, Double> map = new HashMap<>();

            Terms terms = MultiTerms.getTerms(indexReader, field);

            TermsEnum iterate = terms.iterator();

            while (iterate.next() != null) {
                map.put(iterate.term().utf8ToString(), BestTermsInDoc.getValue(indexReader, iterate, order));
            }

            Stream<Map.Entry<String, Double>> sorted = map.entrySet().stream().sorted(Map.Entry.<String, Double>comparingByValue().reversed());

            Object[] arrays = sorted.toArray();

            StringBuilder result = new StringBuilder();
            result.append("Top ").append(top).append(" terms for field ").append(field).append(" ordered by ").append(order).append(" are:\n");
            for(int i = 0; i < top && i< arrays.length; i++){
                String name = arrays[i].toString().split("=")[0];
                String value = arrays[i].toString().split("=")[1];
                result.append(String.format("%-4s", i+1+".")).append(String.format("%-25s",name)).append(String.format("%-20s", value)).append("\n");
            }
            System.out.println(result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
