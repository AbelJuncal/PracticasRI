package es.udc.fi.ri;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Paths;

public class StatsField {

    public static void main(String[] args){
        boolean optionField = false;
        String indexPath = "index";
        String field = null;

        for(int i = 0; i < args.length; i++){
            switch (args[i]){
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-field":
                    optionField = true;
                    field = args[++i];
                    break;
            }
        }

        IndexSearcher searcher;
        try (DirectoryReader reader = DirectoryReader.open(FSDirectory.open((Paths.get(indexPath))))) {
            searcher = new IndexSearcher(reader);
            final FieldInfos fieldInfos = FieldInfos.getMergedFieldInfos(reader);
            CollectionStatistics cs;

            if (optionField) {
                cs = searcher.collectionStatistics(field);
                printStatistics(cs);
            } else {
                for (final FieldInfo fieldInfo : fieldInfos) {
                    cs = searcher.collectionStatistics(fieldInfo.name);
                    printStatistics(cs);
                    //System.out.println(cs);
                }
            }
        } catch (IOException e1) {
            System.out.println("Graceful message: exception " + e1);
            e1.printStackTrace();
        }
    }

    private static void printStatistics(CollectionStatistics cs){
        System.out.println("Name field = " + cs.field());
        System.out.println("Total documents = " + cs.maxDoc());
        System.out.println("Documents that contain this field = " + cs.docCount());
        System.out.println("sum total term freq = " + cs.sumTotalTermFreq());
        System.out.println("sum doc freq = " + cs.sumDocFreq());
        System.out.println();
        System.out.println("-----------------------------------");
        System.out.println();
    }
}
