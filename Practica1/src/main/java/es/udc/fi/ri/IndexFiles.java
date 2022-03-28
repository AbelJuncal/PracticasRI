package es.udc.fi.ri;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.EnumSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.demo.knn.DemoEmbeddings;
import org.apache.lucene.demo.knn.KnnVectorDict;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
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

        IndexThread(Path dir, IndexWriter indexWriter, Boolean isDepth, int depth) {
            this.dir = dir;
            this.indexWriter = indexWriter;
            this.isDepth = isDepth;
            this.depth = depth;
        }

        public void run(){
            try {
                indexDocs(indexWriter, dir, isDepth, depth);
            } catch (IOException e) {
                System.out.println("Aquí botase");
            }

        }

    }
    static final String KNN_DICT = "knn-dict";

    // Calculates embedding vectors for KnnVector search

    /** Index all text files under a directory. */
    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        try{
            prop.load(new FileInputStream("config.properties"));
        } catch (IOException ex){
            ex.printStackTrace();
        }


        String usage = "java org.apache.lucene.demo.IndexFiles"
                + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update] [-knn_dict DICT_PATH]\n\n"
                + "This indexes the documents in DOCS_PATH, creating a Lucene index"
                + "in INDEX_PATH that can be searched with SearchFiles\n"
                + "IF DICT_PATH contains a KnnVector dictionary, the index will also support KnnVector search";
        String indexPath = "index";
        String docsPath = null;
        boolean create = true;
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
                case "-deep":
                    isdepth = true;
                    deep = Integer.parseInt(args[++i]);
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        final ExecutorService executor = Executors.newFixedThreadPool(numCores);

        if (docsPath == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        final Path docDir = Paths.get(docsPath);
        if (!Files.isReadable(docDir)) {
            System.out.println("Document directory '" + docDir.toAbsolutePath()
                    + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        Date start = new Date();
        try {
            System.out.println("Indexing to directory '" + indexPath + "'...");

            Directory dir = FSDirectory.open(Paths.get(indexPath));
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


            IndexWriter writer = new IndexWriter(dir, iwc);
            DirectoryStream<Path> directoryStream = Files.newDirectoryStream(docDir);

            try{
            for (final Path docs : directoryStream){
                if(Files.isDirectory(docs)){
                    final Runnable worker = new IndexThread(docs, writer, isdepth, deep);
                    executor.execute(worker);
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
            writer.close();


            Date end = new Date();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                System.out.println("Indexed " + reader.numDocs() + " documents in " + (end.getTime() - start.getTime())
                        + " milliseconds");
                if (reader.numDocs() > 100 && System.getProperty("smoketester") == null) {
                    throw new RuntimeException(
                            "Are you (ab)using the toy vector dictionary? See the package javadocs to understand why you got this exception.");
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
    static void indexDocs(final IndexWriter writer, Path path, boolean isDepth, int depth) throws IOException {
        if (Files.isDirectory(path)) {
            if(isDepth){
                EnumSet<FileVisitOption> opts = EnumSet.of(FOLLOW_LINKS);
                Files.walkFileTree(path, opts, depth, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        try {
                            indexDoc(writer, file, attrs);
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
                            indexDoc(writer, file, attrs);
                        } catch (@SuppressWarnings("unused") IOException ignore) {
                            ignore.printStackTrace(System.err);
                            // don't index files that can't be read.
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        } else {
            indexDoc(writer, path, Files.readAttributes(path, BasicFileAttributes.class));
        }
    }

    /** Indexes a single document */
    static void indexDoc(IndexWriter writer, Path file, BasicFileAttributes attrs) throws IOException {
        try (InputStream stream = Files.newInputStream(file)) {
            // make a new, empty document
            Document doc = new Document();

            // Add the path of the file as a field named "path". Use a
            // field that is indexed (i.e. searchable), but don't tokenize
            // the field into separate words and don't index term frequency
            // or positional information:
            Field pathField = new StringField("path", file.toString(), Field.Store.YES);
            doc.add(pathField);

            // Add the last modified date of the file a field named "modified".
            // Use a LongPoint that is indexed (i.e. efficiently filterable with
            // PointRangeQuery). This indexes to milli-second resolution, which
            // is often too fine. You could instead create a number based on
            // year/month/day/hour/minutes/seconds, down the resolution you require.
            // For example the long value 2011021714 would mean
            // February 17, 2011, 2-3 PM.


            doc.add(new StringField("lastModifiedTime", String.valueOf(attrs.lastModifiedTime().toMillis()), Field.Store.YES));

            doc.add(new StringField("creationTime", String.valueOf(attrs.creationTime().toMillis()), Field.Store.YES));

            doc.add(new StringField("lastAccessTime", String.valueOf(attrs.lastAccessTime().toMillis()), Field.Store.YES));

            doc.add(new StringField("sizeKB", String.valueOf((Files.size(file)/1024)), Field.Store.YES));

            //Lucene times
            Date creationDate = new Date(attrs.creationTime().toMillis());
            doc.add(new TextField("creationTimeLucene", DateTools.dateToString(creationDate, DateTools.Resolution.MILLISECOND), Field.Store.YES));

            Date lastAccessTimeLucene = new Date(attrs.lastAccessTime().toMillis());
            doc.add(new TextField("lastAccessTimeLucene", DateTools.dateToString(lastAccessTimeLucene, DateTools.Resolution.MILLISECOND), Field.Store.YES));

            Date lastModifiedTimeLucene = new Date(attrs.lastModifiedTime().toMillis());
            doc.add(new TextField("lastModifiedTimeLucene", DateTools.dateToString(lastModifiedTimeLucene, DateTools.Resolution.MILLISECOND), Field.Store.YES));

            // Add the contents of the file to a field named "contents". Specify a Reader,
            // so that the text of the file is tokenized and indexed, but not stored.
            // Note that FileReader expects the file to be in UTF-8 encoding.
            // If that's not the case searching for special characters will fail.
            String contents = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8)).lines().collect(Collectors.joining());

            doc.add(new TextField("contents", contents,Field.Store.NO ));

            doc.add(new TextField("contentsStored", contents, Field.Store.YES));

            //Save the hostname
            doc.add(new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES));

            //Save the thread
            doc.add(new StringField("thread", Thread.currentThread().getName(), Field.Store.YES));

            //Save the type
            doc.add(new StringField("type", fileType(attrs), Field.Store.YES));

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
        }catch(IOException ignored){
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
}
