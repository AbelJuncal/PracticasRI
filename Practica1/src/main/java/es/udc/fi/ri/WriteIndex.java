package es.udc.fi.ri;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class WriteIndex {

    public static void main(String[] args){

        String indexPath = "index";
        String outputpath = null;
        String usage = "java es.udc.fi.ri.WriteIndex"
                + " [-index INDEX_PATH] [-outputfile OUTPUT_FILE_PATH]\n\n";

        for(int i = 0; i < args.length; i++){
            switch (args[i]){
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-outputfile":
                    outputpath = args[++i];
                    break;
            }
        }
        if(outputpath == null){
            System.exit(-1);
        }

        try (DirectoryReader reader = DirectoryReader.open(FSDirectory.open((Paths.get(indexPath)))); FileWriter writer = new FileWriter(outputpath)) {
            int i;
            for (i = 0; i < reader.numDocs(); i++) {
                Document doc = reader.document(i);
                List<IndexableField> fields = doc.getFields();
                writer.write("DocID = " + i + "\n");

                for (IndexableField field : fields) {

                    writer.write(field.name() + " = " + doc.get(field.name()) + "\n");
                }
                writer.write("\n-----------------------------------\n\n");

            }
        } catch (IOException e1) {
            System.out.println("Graceful message: exception " + e1);
            e1.printStackTrace();
        }

    }
}
