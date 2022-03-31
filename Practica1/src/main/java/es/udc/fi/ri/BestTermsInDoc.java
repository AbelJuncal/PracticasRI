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
        String usage = "java es.udc.fi.ri.SimilarDocs"
                + " [-index INDEX_PATH] [-docID] [-field] [-top] [-order df, tf, idf, tfxidf] [-outputfile OUTPUT_FILE_PATH]\n\n";

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

            Map<String, Double> map = new HashMap<>();
            Map<String, List<Double>> fullTuple = new HashMap<>();

            Terms terms = indexReader.getTermVector(Integer.parseInt(docsID), field);

            TermsEnum iterate = terms.iterator();

            Terms otherterms = MultiTerms.getTerms(indexReader, field);
            TermsEnum iterateOtherTerms = otherterms.iterator();
            int indexother = 0;




            while (iterate.next() != null) {
                map.put(iterate.term().utf8ToString(), getValue(indexReader, iterate, order));

                List<Double> allValues = new ArrayList<>();
                double docFreq = iterate.docFreq();
                double idflog10 = Math.log10(indexReader.numDocs()/(docFreq+1))+1;
                allValues.add(docFreq);
                allValues.add((double) iterate.totalTermFreq());
                allValues.add(idflog10);
                allValues.add((double) iterate.totalTermFreq()*idflog10);

                fullTuple.put(iterate.term().utf8ToString(), allValues);


            }

            //System.out.println(map);
            Stream<Map.Entry<String, Double>> sorted = map.entrySet().stream().sorted(Map.Entry.<String, Double>comparingByValue().reversed());
            //System.out.println(Arrays.toString(sorted.toArray()));
            System.out.println(getResults(sorted, top, field, order, fullTuple));
        } catch (IOException e1) {
            System.out.println("Graceful message: exception " + e1);
            e1.printStackTrace();
        }


    }

    public static double getValue(IndexReader reader, TermsEnum term, String option) throws IOException {
        double docFreq = term.docFreq();
        double idflog10 = (int) (Math.log10(reader.numDocs()/(docFreq+1))+1);
        switch (option){
            case "df":
                return term.docFreq();
            case "tf":
                return term.totalTermFreq();
            case "idf":
                return idflog10;
            case "tfxidf":
                return term.totalTermFreq() * idflog10;
        }
        return -1;
    }

    public static String getResults(Stream<Map.Entry<String, Double>> sorted, int n, String field, String order,  Map<String, List<Double>> fullTuple){
        Object[] arrays = sorted.toArray();
        String results = "";
        results = results + ("Top " + n + " terms for field " + field + ", ordered by " + order + ":\n");
        for(int i = 0; i<n && i< arrays.length; i++){
            String name = arrays[i].toString().split("=")[0];
            List<Double> values = fullTuple.get(name);
            results = results.concat( name + "\t" + values.get(0) + "\t" + values.get(1) + "\t" + values.get(2) + "\t" + values.get(3)) + "\n";
        }
        return results;
    }
}
