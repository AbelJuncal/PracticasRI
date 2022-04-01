package es.udc.fi.ri;

import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class BestTermsInDoc {
    public static void main(String[] args) throws IOException {
        String indexPath = null;
        String docID = null;
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
                    docID = args[++i];
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

        if (indexPath == null || docID == null || top == null || order == null || !options.contains(order))  {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        Directory dir = null;
        DirectoryReader indexReader = null;

        try {
            dir = FSDirectory.open(Paths.get(indexPath));
            indexReader = DirectoryReader.open(dir);

            Map<String, Double> map = new HashMap<>();
            Map<String, List<Double>> fullTuple = new HashMap<>();

            Terms terms = indexReader.getTermVector(Integer.parseInt(docID), field);

            TermsEnum iterate = terms.iterator();

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

            Stream<Map.Entry<String, Double>> sorted = map.entrySet().stream().sorted(Map.Entry.<String, Double>comparingByValue().reversed());

            String filename =  indexReader.document(Integer.parseInt(docID)).getField("path").stringValue();
            System.out.println(getResults(sorted, top, field, order, fullTuple, filename));
            if(outputFile != null){
                try(FileWriter fileWriter = new FileWriter(outputFile)){
                    fileWriter.write(getResults(sorted, top, field, order, fullTuple, filename));
                }
            }
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

    public static String getResults(Stream<Map.Entry<String, Double>> sorted, int n, String field, String order,  Map<String, List<Double>> fullTuple, String docId){
        Object[] arrays = sorted.toArray();
        StringBuilder results = new StringBuilder();

        results.append("Top ").append(n).append(" terms for field ").append(field).append(" in doc ").append(docId).append(", ordered by ").append(order).append(":\n\n");
        results.append(String.format("%-23s", "Term")).append(String.format("%-11s", "df")).append(String.format("%-10s", "tf"));
        results.append(String.format("%-9s", "idflog10")).append(String.format("%-9s", " tf x idflog10")).append("\n");
        results.append("--------------------------------------------------------------------").append("\n");

        for(int i = 0; i<n && i< arrays.length; i++){
            String name = arrays[i].toString().split("=")[0];
            List<Double> values = fullTuple.get(name);
            results.append(String.format("%-4s", i+1+".")).append(String.format("%-15s", name)).append("\t").append(String.format("%10.2f", values.get(0)));
            results.append("\t").append(String.format("%10.2f", values.get(1))).append("\t");
            results.append(String.format("%10.2f", values.get(2))).append("\t").append(String.format("%10.2f", values.get(3))).append("\n");
        }

        return results.toString();
    }
}
