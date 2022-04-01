package es.udc.fi.ri;

import org.apache.commons.math3.linear.RealVector;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;

//The code from DocClusters and Data Set classes was taken from Darinka Xobenica github (github.com/mentathiel)

public class DocClusters {
    public static void main(String[] args) throws IOException {

        String indexPath = null;
        String docsID = null;
        String field = null;
        Integer top = null;
        String rep = null;
        Integer k = null;
        String usage = "java es.udc.fi.ri.SimilarDocs"
                + " [-index INDEX_PATH] [-doc DOCS_PATH] [-field] [-top] [-rep bin, tf, tfxidf] [-k]\n\n";

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
                case "-k":
                    k = Integer.parseInt(args[++i]);
                    break;
            }
        }

        if(indexPath == null || k == null | docsID == null | field == null | top == null | !options.contains(rep)){
            System.out.println("Usage " + usage);
            System.exit(1);
        }

        SimilarDocs.main(args);

        List<RealVector> docsVector = SimilarDocs.docsVector;

        try{
            File result = new File ("results.csv");
            FileWriter writer = new FileWriter(result);
            BufferedWriter bufWriter = new BufferedWriter(writer);

            int x;
            for (x = 0; x < docsVector.get(0).getDimension() - 1; ++x)
                bufWriter.write("Term" + x + ",\t");
            bufWriter.write("Term" + x + "\n");

            int y = 0;
            for (RealVector docVector : docsVector){
                if(y<top) {
                    double[] listVect = docVector.toArray();
                    for (x = 0; x < listVect.length - 1; ++x) {
                        bufWriter.write(BigDecimal.valueOf(listVect[x])
                                .setScale(2, RoundingMode.HALF_UP).doubleValue() + ",\t\t");
                    }
                    bufWriter.write(BigDecimal.valueOf(listVect[x])
                            .setScale(2, RoundingMode.HALF_UP).doubleValue() + "\n");
                    y++;
                }
            }

            bufWriter.close();
            DataSet dataSet = new DataSet("results.csv");
            KMeans.kmeans(dataSet, k);
            if (!result.delete()){
                System.out.println("Imposible directory delete");
            }
            System.out.println("Creating and executin " + k + " DocClusters:");
            System.out.println(dataSet);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
