package es.udc.fi.ri;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.lang.reflect.Field;
import java.nio.file.Paths;

public class StatsField {


    public static void main(String[] args) throws Exception{
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

        DirectoryReader reader = DirectoryReader.open(FSDirectory.open((Paths.get(indexPath))));
        IndexSearcher searcher = new IndexSearcher(reader);
        final FieldInfos fieldInfos = FieldInfos.getMergedFieldInfos(reader);
        CollectionStatistics cs;


        if(optionField){
            cs = searcher.collectionStatistics(field);
            System.out.println(cs.toString());
        }else {
            for(final FieldInfo fieldInfo : fieldInfos){
                cs = searcher.collectionStatistics(fieldInfo.name);
                System.out.println(cs.toString());
            }
        }
    }

}
