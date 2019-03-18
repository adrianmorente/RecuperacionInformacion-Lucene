package adrianmorente.giw.sri.indexer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;


/**
 * Clase indexadora del sistema de Recuperación de Información basado
 * en Lucene. recibirá como argumentos la ruta de la colección documental a 
 * indexar, el fichero de palabras vacías a emplear y la ruta donde alojar los
 * índices, y llevará a cabo la indexación, creando los índices oportunos y 
 * ficheros auxiliares necesarios para la recuperación.
 * @author adrianmorente
 */
public class LuceneIndexer {
    
    /**
     * Constructor without parameters. Unused.
     */
    private LuceneIndexer(){}
    
    /**
     * Indexes a single document from given
     * @param writer
     * @param file
     * @param lastModified 
     */
    private static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
        try(InputStream stream = Files.newInputStream(file)){
            Document doc = new Document();
            
            // Stores the path to the indexed document
            Field pathField = new StringField("path", file.toString(), Field.Store.YES);
            doc.add(pathField);
            
            // Stores the content of the indexed document
            doc.add(new TextField("content", new BufferedReader(
                new InputStreamReader(stream, StandardCharsets.UTF_8)
            )));
            
            // Creates or updated the indexed document
            if(writer.getConfig().getOpenMode() == OpenMode.CREATE){
                System.out.println("adding " + file);
                writer.addDocument(doc);
            } else {
                System.out.println("updating " + file);
                writer.updateDocument(new Term("path", file.toString()), doc);
            }
        }
    }
    
    /**
     * Indexes the documents under the given directory
     * @param writer
     * @param path 
     */
    private static void indexDocs(final IndexWriter writer, Path path) throws IOException {
        if(Files.isDirectory(path)){
            Files.walkFileTree(path, new SimpleFileVisitor<Path>(){
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    try {
                        indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
                    } catch (IOException ignore) {
                        // not read file, not indexed file 
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } else
            indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        // Correct use of the application
        String usage = "adrianmorente.giw.sri.indexer"
                + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update]\n\n";
        
        // Directory where the indexes will be stored
        String indexPath = "index";
        
        // Directory where the documents will be read from
        String docsPath = null;
        
        // It determines if the old index has to be removed
        boolean create = true;
        
        // Iterates on the arguments to get the new values for paths
        for(int i=0; i<args.length; i++) {
            if("-index".equals(args[i])){
                indexPath = args[i+1];
                i++;
            } else if("-docs".equals(args[i])){
                docsPath = args[i+1];
                i++;
            } else if("-update".equals(args[i]))
                create = false;
        }
        
        // Needs to receive a path to documents
        if(docsPath == null){
            System.err.println("Uso del programa: " + usage);
            System.exit(1);
        }
        
        // Checks the legibility of the documents path
        final Path docDir = Paths.get(docsPath);
        if(!Files.isReadable(docDir)){
            System.out.println("El directorio de documentos no existe "
                    + "o no es legible, revise el directorio introducido.");
            System.exit(1);
        }
        
        // Start to count the time
        Date start = new Date();

        // Do the indexing
        try {
            System.out.println("Guardando índices en directorio " 
                    + indexPath + "...");
            Directory dir = FSDirectory.open(Paths.get(indexPath));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            
            // Mode of running the program
            config.setOpenMode( create ? OpenMode.CREATE : OpenMode.CREATE_OR_APPEND);
            
            // Index writer
            IndexWriter writer = new IndexWriter(dir, config);
            indexDocs(writer, docDir);
            writer.close();
            
            Date end = new Date();
            System.out.println(end.getTime() - start.getTime() + " ms en total.");
        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass() +
                "\n with message: " + e.getMessage());
        }
    }

}
