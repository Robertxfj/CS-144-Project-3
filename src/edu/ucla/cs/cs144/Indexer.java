package edu.ucla.cs.cs144;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.sql.*;

public class Indexer {
    
    /** Creates a new instance of Indexer */
    public Indexer() {
    }
 
    public void rebuildIndexes() {

        Connection conn = null;

        // create a connection to the database to retrieve Items from MySQL
        try {
            conn = DbManager.getConnection(true);

            /*
             * Add your code here to retrieve Items using the connection
             * and add corresponding entries to your Lucene inverted indexes.
             *
             * You will have to use JDBC API to retrieve MySQL data from Java.
             * Read our tutorial on JDBC if you do not know how to use JDBC.
             *
             * You will also have to use Lucene IndexWriter and Document
             * classes to create an index and populate it with Items data.
             * Read our tutorial on Lucene as well if you don't know how.
             *
             * As part of this development, you may want to add
             * new methods and create additional Java classes.
             * If you create new classes, make sure that
             * the classes become part of "edu.ucla.cs.cs144" package
             * and place your class source files at src/edu/ucla/cs/cs144/.
             *
             */

            Directory indexDir = FSDirectory.open(new File("/var/lib/lucene/index1"));
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_2, new StandardAnalyzer());
            IndexWriter indexWriter = new IndexWriter(indexDir, config);

            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM Item");
            while (resultSet.next()) {
                Document document = new Document();
                document.add(new IntField("id", resultSet.getInt("id"), Field.Store.YES));
                document.add(new StringField("name", resultSet.getString("name"), Field.Store.YES));
                document.add(new TextField("description", resultSet.getString("description"), Field.Store.NO));
                indexWriter.addDocument(document);
            }

            indexWriter.close();

            // close the database connection
            conn.close();
        } catch (SQLException ex) {
            System.out.println(ex);
        } catch (IOException ex) {
            System.out.println(ex);
        }



    }    

    public static void main(String args[]) {
        Indexer idx = new Indexer();
        idx.rebuildIndexes();
    }   
}
