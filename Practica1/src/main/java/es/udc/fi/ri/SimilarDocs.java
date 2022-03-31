package es.udc.fi.ri;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class SimilarDocs {
    public static RealVector targetDocVector;
    public static List<RealVector> docsVector = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        String indexPath = "index";
        String docsID = null;
        String field = null;
        Integer top = null;
        String rep = null;
        String usage = "java es.udc.fi.ri.SimilarDocs"
                + " [-index INDEX_PATH] [-doc DOCS_PATH] [-field] [-top] [-rep bin, tf, tfxidf]\n\n";

        List<String> options = Arrays.asList("bin", "tf", "tfxidf");

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

        if(docsID == null | field == null | top == null | !options.contains(rep)){
            System.out.print("Bad arguments");
            System.exit(1);
        }

        Directory dir = null;
        DirectoryReader indexReader = null;
        Set<String> terms = new HashSet<>();
        Map<String, Double> map = new HashMap<>();

        StringBuilder result = new StringBuilder();

        try{
            dir = FSDirectory.open(Paths.get(indexPath));
            indexReader = DirectoryReader.open(dir);

            result.append("The top ").append(top).append(" similar docs to ");
            result.append(indexReader.document(Integer.parseInt(docsID)).getField("path").stringValue());
            result.append(" ordered by ").append(rep).append(" representation is: \n");

            Terms documentTerms = indexReader.getTermVector(Integer.parseInt(docsID), field);
            TermsEnum documentTermsEnum = null;
            documentTermsEnum = documentTerms.iterator();
            Map<String, Double> documentFrequencies = new HashMap<>();
            BytesRef doctext = null;
            RealVector docvaux = toRealVector(documentFrequencies, terms);
            targetDocVector = docvaux;

            while ((doctext = documentTermsEnum.next())!=null){
                String term = doctext.utf8ToString();
                //double freq = documentTermsEnum.totalTermFreq();
                double freq = getValue(indexReader, documentTermsEnum, rep, null, null);
                documentFrequencies.put(term, freq);
                terms.add(term);
            }

            for (int i = 0; i < indexReader.numDocs(); i++) {
                Document doc = indexReader.document(i);
                Terms auxterm = indexReader.getTermVector(i, field);
                TermsEnum termsEnum = null;
                termsEnum = auxterm.iterator();
                Map<String, Double> frequencies = new HashMap<>();
                BytesRef text = null;
                Set<String> auxterms = new HashSet<>();


                while ((text = termsEnum.next())!=null){
                    String term = text.utf8ToString();
                    //double freq = termsEnum.totalTermFreq();
                    double freq = getValue(indexReader, termsEnum, rep, term, terms);
                    frequencies.put(term, freq);
                    auxterms.add(term);
                }

                auxterms.addAll(terms);

                RealVector vaux = toRealVector(frequencies, auxterms);
                docvaux = toRealVector(documentFrequencies, auxterms);

                docsVector.add(vaux);

                double c = getCosineSimilarity(docvaux,vaux);
                map.put(doc.getField("path").stringValue(), c);

                //System.out.println(indexReader.document(Integer.parseInt(docsID)).getField("path").stringValue() + "\t" + doc.getField("path").stringValue() + "\t" + c);
            }
            Stream<Map.Entry<String, Double>> sorted = map.entrySet().stream().sorted(Map.Entry.<String, Double>comparingByValue().reversed());
            Object[] arrays = sorted.toArray();
            for (int i = 0; i<top && i<arrays.length; i++){
                String name = arrays[i].toString().split("=")[0];
                Double value = Double.parseDouble(arrays[i].toString().split("=")[1]);
                result.append(String.format("%-40s", name)).append("\t").append(String.format("%20.2f", value)).append("\n");
            }
            System.out.println(result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static double getCosineSimilarity(RealVector v1, RealVector v2){
        return (v1.dotProduct(v2)) / (v1.getNorm() * v2.getNorm());
    }

    public static RealVector toRealVector(Map<String, Double> map, Set<String> terms){
        RealVector vector = new ArrayRealVector( terms.size());
        int x=0;
        for (String term:terms){
            double value = map.containsKey(term) ? map.get(term) : 0;
            vector.setEntry(x++, value);
        }

        return (RealVector) vector.mapDivide(vector.getL1Norm());
    }

    public static double getValue(IndexReader reader, TermsEnum term, String option, String value, Set<String> terms) throws IOException {
        double docFreq = term.docFreq();
        double idflog10 = (int) (Math.log10(reader.numDocs()/(docFreq+1))+1);
        double bin = 0;

        if(terms != null && value != null) {
            if (terms.contains(value)) {
                bin = 1;
            } else {
                bin = 0;
            }
        }

        switch (option){
            case "tf":
                return term.totalTermFreq();
            case "tfxidf":
                return term.totalTermFreq() * idflog10;
            case "bin":
                return bin;
        }
        return -1;
    }
}
