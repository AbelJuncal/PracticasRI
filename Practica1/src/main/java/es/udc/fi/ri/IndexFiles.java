package es.udc.fi.ri;

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;

import static java.nio.file.FileVisitOption.FOLLOW_LINKS;

/**
 * Index all text files under a directory.
 *
 * <p>
 * This is a command-line application demonstrating simple Lucene indexing. Run
 * it with no command-line arguments for usage information.
 */
public class IndexFiles implements AutoCloseable {
    @Override
    public void close() throws Exception {

    }

    public static class IndexThread implements Runnable {

        IndexWriter indexWriter;
        Path dir;
        Boolean isDepth;
        Integer depth;
        List<String> fileformats;
        Integer onlyTopLines;
        Integer onlyBottomLines;
        String indexDir;
        Boolean partialindexes;
        Boolean create;

        IndexThread(Path dir, IndexWriter indexWriter, Boolean isDepth, int depth, List<String> fileformats, Integer onlyTopLines, Integer onlyBottomLines, String indexDir,  Boolean partialindexes, Boolean create) {
            this.indexWriter = indexWriter;
            this.dir = dir;
            this.isDepth = isDepth;
            this.depth = depth;
            this.fileformats = fileformats;
            this.onlyTopLines = onlyTopLines;
            this.onlyBottomLines = onlyBottomLines;
            this.partialindexes = partialindexes;
            this.indexDir = indexDir;
            this.create = create;
        }


        IndexThread(Path dir, Boolean isDepth, int depth, List<String> fileformats, Integer onlyTopLines, Integer onlyBottomLines, String indexDir,  Boolean partialindexes, Boolean create) {
            this.dir = dir;
            this.isDepth = isDepth;
            this.depth = depth;
            this.fileformats = fileformats;
            this.onlyTopLines = onlyTopLines;
            this.onlyBottomLines = onlyBottomLines;
            this.partialindexes = partialindexes;
            this.indexDir = indexDir;
            this.create = create;
        }

        public void run(){
            try {
                if(partialindexes){
                    Analyzer analyzer = new StandardAnalyzer();
                    Directory directory = FSDirectory.open(Paths.get(indexDir + "/" + dir.getFileName()));
                    IndexWriterConfig config = new IndexWriterConfig(analyzer);

                    if (create) {
                        // Create a new index in the directory, removing any
                        // previously indexed documents:
                        config.setOpenMode(OpenMode.CREATE);
                    } else {
                        // Add new documents to an existing index:
                        config.setOpenMode(OpenMode.CREATE_OR_APPEND);
                    }

                    IndexWriter iwriter = null;
                    try{
                        iwriter = new IndexWriter(directory, config);
                    } catch (IOException e){
                        e.printStackTrace();
                    }
                    try {
                        indexDocs(iwriter, dir, isDepth, depth, fileformats, onlyTopLines, onlyBottomLines);
                        iwriter.commit();
                        iwriter.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }else{
                    indexDocs(indexWriter, dir, isDepth, depth, fileformats, onlyTopLines, onlyBottomLines);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }
    static final String KNN_DICT = "knn-dict";

    // Calculates embedding vectors for KnnVector search

    /** Index all text files under a directory. */
    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = loader.getResourceAsStream("config.properties");
        prop.load(inputStream);
        String onlyFiles = prop.getProperty("onlyFiles");
        Integer onlyTopLines = (prop.getProperty("onlyTopLines") != null) ?  Integer.parseInt(prop.getProperty("onlyTopLines")) : null;
        Integer onlyBottomLines = (prop.getProperty("onlyBottomLines") != null) ?  Integer.parseInt(prop.getProperty("onlyBottomLines")) : null;

        List<String> formatAccepted = new ArrayList<>();

        if(onlyFiles != null){
            StringTokenizer st = new StringTokenizer(onlyFiles);
            while (st.hasMoreTokens()){
                formatAccepted.add(st.nextToken());
            }
        }

        if (onlyTopLines == null && onlyBottomLines == null){
            onlyTopLines = -1;
            onlyBottomLines = -1;
        }else if (onlyTopLines == null){
            onlyTopLines = 0;
        }else if(onlyBottomLines == null){
            onlyBottomLines = 0;
        }


        String usage = "java org.apache.lucene.demo.IndexFiles"
                + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update] [-knn_dict DICT_PATH]\n\n"
                + "This indexes the documents in DOCS_PATH, creating a Lucene index"
                + "in INDEX_PATH that can be searched with SearchFiles\n"
                + "IF DICT_PATH contains a KnnVector dictionary, the index will also support KnnVector search";
        String indexPath = null;
        String docsPath = null;
        boolean create = true;
        boolean partialIndexes = false;
        int numCores = Runtime.getRuntime().availableProcessors();

        boolean isdepth = false;
        int deep = 0;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docs":
                    docsPath = args[++i];
                    break;
                case "-update":
                    create = false;
                    break;
                case "-create":
                    create = true;
                    break;
                case "-numThreads":
                    numCores = Integer.parseInt(args[++i]);
                    break;
                case "-partialIndexes":
                    partialIndexes = true;
                    break;
                case "-deep":
                    isdepth = true;
                    deep = Integer.parseInt(args[++i]);
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        final ExecutorService executor = Executors.newFixedThreadPool(numCores);

        if (indexPath == null || docsPath == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        final Path docDir = Paths.get(docsPath);
        if (!Files.isReadable(docDir)) {
            System.out.println("Document directory '" + docDir.toAbsolutePath()
                    + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        IndexWriter writer = null;
        Directory dir = null;
        deleteDirectoryRecursion(Paths.get(indexPath));

        Date start = new Date();
        try {
            System.out.println("Indexing to directory '" + indexPath + "'...");

            if(!partialIndexes) {

                dir = FSDirectory.open(Paths.get(indexPath));
                Analyzer analyzer = new StandardAnalyzer();
                IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

                if (create) {
                    // Create a new index in the directory, removing any
                    // previously indexed documents:
                    iwc.setOpenMode(OpenMode.CREATE);
                } else {
                    // Add new documents to an existing index:
                    iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
                }

                // Optional: for better indexing performance, if you
                // are indexing many documents, increase the RAM
                // buffer. But if you do this, increase the max heap
                // size to the JVM (eg add -Xmx512m or -Xmx1g):
                //
                // iwc.setRAMBufferSizeMB(256.0);


                writer = new IndexWriter(dir, iwc);

            }
            DirectoryStream<Path> directoryStream = Files.newDirectoryStream(docDir);

            try{
            for (final Path docs : directoryStream){
                if(Files.isDirectory(docs)){
                    if(partialIndexes){
                        final Runnable worker = new IndexThread(docs, isdepth, deep, formatAccepted, onlyTopLines, onlyBottomLines, indexPath, partialIndexes, create);
                        executor.execute(worker);
                    }else {
                        final Runnable worker = new IndexThread(docs, writer, isdepth, deep, formatAccepted, onlyTopLines, onlyBottomLines, indexPath, partialIndexes, create);
                        executor.execute(worker);
                    }
                }
            }

            } finally{
                try {
                    IOUtils.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

            executor.shutdown();

            try {
                executor.awaitTermination(1, TimeUnit.HOURS);
            } catch (final InterruptedException e) {
                e.printStackTrace();
                System.exit(-2);
            }

            if(!partialIndexes){
                writer.close();
            }

            if(partialIndexes){
                IndexWriterConfig iconfig = new IndexWriterConfig(new StandardAnalyzer());
                IndexWriter ifusedwriter = null;

                MMapDirectory dir3 = null;
                try{
                    dir3 = new MMapDirectory(Paths.get(indexPath));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try{
                    ifusedwriter = new IndexWriter(dir3, iconfig);

                    DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(indexPath));
                    for(final Path path : stream){
                        if(Files.isDirectory(path)){
                            Directory auxdir = FSDirectory.open(path);
                            ifusedwriter.addIndexes(auxdir);
                            deleteDirectoryRecursion(path);
                        }
                    }

                    ifusedwriter.commit();
                    ifusedwriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                Date end = new Date();
                try (IndexReader reader = DirectoryReader.open(dir3)) {
                    System.out.println("Indexed " + reader.numDocs() + " documents in " + (end.getTime() - start.getTime())
                            + " milliseconds");
                }
            }else{
                Date end = new Date();
                try (IndexReader reader = DirectoryReader.open(dir)) {
                    System.out.println("Indexed " + reader.numDocs() + " documents in " + (end.getTime() - start.getTime())
                            + " milliseconds");
                }
            }
        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
        }
    }

    /**
     * Indexes the given file using the given writer, or if a directory is given,
     * recurses over files and directories found under the given directory.
     *
     * <p>
     * NOTE: This method indexes one document per input file. This is slow. For good
     * throughput, put multiple documents into your input file(s). An example of
     * this is in the benchmark module, which can create "line doc" files, one
     * document per line, using the <a href=
     * "../../../../../contrib-benchmark/org/apache/lucene/benchmark/byTask/tasks/WriteLineDocTask.html"
     * >WriteLineDocTask</a>.
     *
     * @param writer Writer to the index where the given file/dir info will be
     *               stored
     * @param path   The file to index, or the directory to recurse into to find
     *               files to indt
     * @throws IOException If there is a low-level I/O error
     */
    static void indexDocs(final IndexWriter writer, Path path, boolean isDepth, int depth, List<String> format, int onlyTopLines, int onlyBottomLines) throws IOException {
        if (Files.isDirectory(path)) {
            if(isDepth){;
                EnumSet<FileVisitOption> opts = EnumSet.of(FOLLOW_LINKS);
                Files.walkFileTree(path, opts, depth, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        try {
                            indexDoc(writer, file, attrs, format, onlyTopLines, onlyBottomLines);
                        } catch (@SuppressWarnings("unused") IOException ignore) {
                            ignore.printStackTrace(System.err);
                            // don't index files that can't be read.
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });

            }else{
                Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        try {
                            indexDoc(writer, file, attrs, format, onlyTopLines, onlyBottomLines);
                        } catch (@SuppressWarnings("unused") IOException ignore) {
                            ignore.printStackTrace(System.err);
                            // don't index files that can't be read.
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        } else {
            indexDoc(writer, path, Files.readAttributes(path, BasicFileAttributes.class), format, onlyTopLines, onlyBottomLines);
        }
    }

    /** Indexes a single document */
    static void indexDoc(IndexWriter writer, Path file, BasicFileAttributes attrs, List<String> format, int onlyTopLines, int onlyBottomLines) throws IOException {
        String extension = "";
        int index = file.toString().lastIndexOf('.');
        if (index > 0) {
            extension = file.toString().substring(index);
        }

        if(format.isEmpty() || format.contains(extension)) {
            try (InputStream stream = Files.newInputStream(file)) {
                // make a new, empty document
                Document doc = new Document();

                Field pathField = new StringField("path", file.toString(), Field.Store.YES);
                doc.add(pathField);

                String formato = "yyyy-MM-dd HH:mm:ss";
                SimpleDateFormat formateo = new SimpleDateFormat(formato);
                String lastModifiedTime = formateo.format(new Date(attrs.lastModifiedTime().toMillis()));
                String creationTime = formateo.format(new Date(attrs.creationTime().toMillis()));
                String lastAccessTime = formateo.format(new Date(attrs.lastAccessTime().toMillis()));

                doc.add(new Field("lastModifiedTime", lastModifiedTime, typeb));

                doc.add(new Field("creationTime", creationTime, typeb));

                doc.add(new Field("lastAccessTime", lastAccessTime, typeb));

                doc.add(new Field("sizeKB", String.valueOf((Files.size(file) / 1024)), typeb));

                Date creationTimeLucene = new Date(attrs.creationTime().toMillis());
                Date lastAccessTimeLucene = new Date(attrs.lastAccessTime().toMillis());
                Date lastModifiedTimeLucene = new Date(attrs.lastModifiedTime().toMillis());

                doc.add(new Field("creationTimeLucene", DateTools.dateToString(creationTimeLucene, DateTools.Resolution.MILLISECOND), typeb));
                doc.add(new Field("lastAccessTimeLucene", DateTools.dateToString(lastAccessTimeLucene, DateTools.Resolution.MILLISECOND), typeb));
                doc.add(new Field("lastModifiedTimeLucene", DateTools.dateToString(lastModifiedTimeLucene, DateTools.Resolution.MILLISECOND), typeb));

                String contents;

                if(onlyTopLines == -1 && onlyBottomLines == -1){
                    contents = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8)).lines().collect(Collectors.joining());
                }else{
                    contents = "";
                    InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8);
                    BufferedReader br = new BufferedReader(reader);
                    int firstLines = onlyTopLines;
                    String line;
                    while ((line = br.readLine()) != null && firstLines>0){
                        contents = contents.concat(line);
                        firstLines --;
                    }

                    BufferedReader brlast = new BufferedReader(reader);
                    int lines = (int) Files.lines(file).count();
                    int lastLines = lines - onlyBottomLines;
                    int readlines = 0;

                    while ((line = brlast.readLine()) != null && readlines<=lines){
                        if(readlines > lastLines) {
                            contents = contents.concat(line);
                            lastLines++;
                        }
                    }
                }

                doc.add(new Field("contents", contents, typec));

                doc.add(new Field("contentsStored", contents, typea));

                doc.add(new Field("hostname", InetAddress.getLocalHost().getHostName(), typeb));

                doc.add(new Field( "thread", Thread.currentThread().getName(), typeb));

                doc.add(new Field("type",fileType(attrs), typeb));

                if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                    // New index, so we just add the document (no old document can be there):
                    System.out.println("adding " + file);
                    writer.addDocument(doc);
                } else {
                    // Existing index (an old copy of this document may have been indexed) so
                    // we use updateDocument instead to replace the old one matching the exact
                    // path, if present:
                    System.out.println("updating " + file);
                    writer.updateDocument(new Term("path", file.toString()), doc);
                }
            } catch (IOException ignored) {
            }
        }
    }

    public static String fileType(BasicFileAttributes attributes){
        if(attributes.isDirectory()){
            return "directory";
        }else if(attributes.isRegularFile()){
            return "regular file";
        }else if(attributes.isSymbolicLink()){
            return "symbolic link";
        }else if(attributes.isOther()){
            return "other";
        }else{
            return "not-known type";
        }
    }

    public static final FieldType typea = new FieldType();
    public static final FieldType typeb = new FieldType();
    public static final FieldType typec = new FieldType();

    static{
        typea.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        typea.setTokenized(true);
        typea.setStored(true);
        typea.setStoreTermVectors(true);
        typea.setStoreTermVectorPositions(true);
        typea.freeze();

        typeb.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        typeb.setTokenized(false);
        typeb.setStored(true);
        typeb.setStoreTermVectors(true);
        typeb.freeze();

        typec.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        typec.setStored(false);
        typec.setTokenized(true);
        typec.setStoreTermVectors(true);
        typec.freeze();


    }

    public static void deleteDirectoryRecursion(Path path) throws IOException {
        if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(path)) {
                for (Path entry : entries) {
                    deleteDirectoryRecursion(entry);
                }
            }
        }
        Files.delete(path);
    }
}
