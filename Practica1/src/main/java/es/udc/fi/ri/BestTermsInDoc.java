package es.udc.fi.ri;

import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class BestTermsInDoc {
    public static void main(String[] args) throws IOException {
        String indexPath = "index";
        String docsID = null;
        String field = null;
        Integer top = null;
        String order = null;
        String outputFile = null;

        List<String> options = Arrays.asList("df", "tf", "idf", "tfxidf");

        for(int i = 0; i < args.length; i++){
            switch (args[i]){
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docID":
                    docsID = args[++i];
                    break;
                case "-field":
                    field = args[++i];
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;
                case "-order":
                    order = args[++i];
                    break;
                case "-outputfile":
                    outputFile = args[++i];
                    break;

            }
        }

        if (top == null || order == null || !options.contains(order))  {
            //System.err.println("Usage: " + usage);
            System.exit(1);
        }

        Directory dir = null;
        DirectoryReader indexReader = null;

        if(docsID == null){
            //System.err.println("Usage: " + usage);
            System.exit(1);
        }

        try {
            dir = FSDirectory.open(Paths.get(indexPath));
            indexReader = DirectoryReader.open(dir);

            Map<TermsEnum, Integer> map = new HashMap<>();

            Terms terms = indexReader.getTermVector(Integer.parseInt(docsID), field);

            TermsEnum iterate = terms.iterator();

            while (iterate.next() != null) {
                map.put(iterate, getValue(indexReader, iterate, order));
            }

            //System.out.println(map);
            Stream<Map.Entry<TermsEnum, Integer>> sorted = map.entrySet().stream().sorted(Map.Entry.<TermsEnum, Integer>comparingByValue().reversed());

            System.out.println(Arrays.toString(sorted.toArray()));
        } catch (IOException e1) {
            System.out.println("Graceful message: exception " + e1);
            e1.printStackTrace();
        }


    }

    public static Integer getValue(IndexReader reader, TermsEnum term, String option) throws IOException {
        int docFreq = term.docFreq();
        int idflog10 = (int) (Math.log10(reader.numDocs()/(docFreq+1))+1);
        switch (option){
            case "df":
                return term.docFreq();
            case "tf":
                return (int) term.totalTermFreq();
            case "idf":
                return idflog10;
            case "tfxidf":
                return (int) (term.totalTermFreq() * idflog10);
        }
        return -1;
    }

    public static String getResults(Stream<Map.Entry<String, Integer>> sorted, int n, String field, String order, String term, int tf, int df, int idflog10, int tfxidfglog10){
        String results = "";
        results = results + ("Top " + n + "terms for field " + field + ", ordered by " + order + ":\n");
        results = results.concat(term + "\t" + tf + "\t" + df + "\t" + idflog10 + "\t" + tfxidfglog10) + "\n";
        return results;
    }
}
