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

    public static void main(String[] args) {
        String indexPath = null;
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

        if(indexPath == null | docsID == null | field == null | top == null | !options.contains(rep)){
            System.out.print("Usage" + usage);
            System.exit(1);
        }

        Directory dir;
        DirectoryReader indexReader;
        Set<String> terms = new HashSet<>();
        Set<String> docTerms = new HashSet<>();
        Map<String, Double> map = new HashMap<>();

        StringBuilder result = new StringBuilder();

        try{
            dir = FSDirectory.open(Paths.get(indexPath));
            indexReader = DirectoryReader.open(dir);
            String docsIDfilename = indexReader.document(Integer.parseInt(docsID)).getField("path").stringValue();

            result.append("The top ").append(top).append(" similar docs to ");
            result.append(docsIDfilename);
            result.append(" ordered by ").append(rep).append(" representation is: \n");

            Terms documentTerms = indexReader.getTermVector(Integer.parseInt(docsID), field);
            TermsEnum documentTermsEnum;
            documentTermsEnum = documentTerms.iterator();
            Map<String, Double> documentFrequencies = new HashMap<>();
            BytesRef doctext;
            RealVector docvaux = toRealVector(documentFrequencies, terms);
            targetDocVector = docvaux;

            while ((doctext = documentTermsEnum.next())!=null){
                String term = doctext.utf8ToString();
                double freq;
                if(rep.equals("bin")){
                        freq = 1;
                }else {
                    freq = getValue(indexReader, documentTermsEnum, rep);
                }
                documentFrequencies.put(term, freq);
                docTerms.add(term);
                terms.add(term);
            }

            for (int i = 0; i < indexReader.numDocs(); i++) {
                Document doc = indexReader.document(i);
                String docFilename = doc.getField("path").stringValue();
                if(!docsIDfilename.equals(docFilename)) {
                    Terms auxterm = indexReader.getTermVector(i, field);
                    TermsEnum termsEnum;
                    termsEnum = auxterm.iterator();
                    BytesRef text;

                    while ((text = termsEnum.next()) != null) {
                        String term = text.utf8ToString();
                        terms.add(term);
                    }
                }
            }

            for (int i = 0; i < indexReader.numDocs(); i++) {
                Document doc = indexReader.document(i);
                String docFilename = doc.getField("path").stringValue();

                if(!docsIDfilename.equals(docFilename)) {
                    Terms auxterm = indexReader.getTermVector(i, field);
                    TermsEnum termsEnum;
                    termsEnum = auxterm.iterator();
                    Map<String, Double> frequencies = new HashMap<>();
                    BytesRef text;

                    while ((text = termsEnum.next()) != null) {
                        String term = text.utf8ToString();
                        double freq = 0;
                        if (rep.equals("bin")) {
                            if (docTerms.contains(term)) {
                                freq = 1;
                            }
                        } else {
                            freq = getValue(indexReader, termsEnum, rep);
                        }
                        frequencies.put(term, freq);
                    }
                    RealVector vaux = toRealVector(frequencies, terms);
                    docvaux = toRealVector(documentFrequencies, terms);


                    docsVector.add(vaux);

                    double c = getCosineSimilarity(docvaux, vaux);
                    map.put(doc.getField("path").stringValue(), c);
                }

            }

            Stream<Map.Entry<String, Double>> sorted = map.entrySet().stream().sorted(Map.Entry.<String, Double>comparingByValue().reversed());
            Object[] arrays = sorted.toArray();
            for (int i = 0; i<top && i<arrays.length; i++){
                String[] split = arrays[i].toString().split("=");
                String name = split[0];
                Double value = Double.parseDouble(split[1]);
                result.append(String.format("%-4s", i+1+".")).append(String.format("%-40s", name)).append("\t").append(String.format("%20.2f", value)).append("\n");
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

        return vector.mapDivide(vector.getL1Norm());
    }

    public static double getValue(IndexReader reader, TermsEnum term, String option) throws IOException {
        double docFreq = term.docFreq();
        double idflog10 = (int) (Math.log10(reader.numDocs()/(docFreq+1))+1);

        switch (option){
            case "tf":
                return term.totalTermFreq();
            case "tfxidf":
                return term.totalTermFreq() * idflog10;
        }
        return -1;
    }
}
