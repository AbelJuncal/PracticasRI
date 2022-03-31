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

public class DocClusters {
    public static void main(String[] args) throws IOException {

        String indexPath = "index";
        String docsID = null;
        String field = null;
        Integer top = null;
        String rep = null;
        Integer k = null;

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

        if(k == null | docsID == null | field == null | top == null | !options.contains(rep)){
            System.exit(1);
        }

        SimilarDocs.main(args);

        RealVector targetVector = SimilarDocs.targetDocVector;
        List<RealVector> docsVector = SimilarDocs.docsVector;

        System.out.println(k);

        try{
            File result = new File ("clusterResult.csv");
            FileWriter writer = new FileWriter(result);
            BufferedWriter bufWriter = new BufferedWriter(writer);
            int x=0;

            while(x < docsVector.get(0).getDimension()){
                bufWriter.write("TermName" + x + ",\t");
                x++;
            }

            double[] listVect;
            for (RealVector docVector : docsVector){
                listVect = docVector.toArray();
                for(x=0; x < listVect.length; x++){
                    bufWriter.write(BigDecimal.valueOf(listVect[x])
                            .setScale(2, RoundingMode.HALF_UP).doubleValue() + ",\t\t");
                }
            }

            bufWriter.close();
            DataSet dataSet = new DataSet("clusterResult.csv");
            KMeans.kmeans(dataSet, k);
            if (!result.delete()){
                System.out.println("Imposible borrar csv autogenerado");
            }
            System.out.println(dataSet);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
