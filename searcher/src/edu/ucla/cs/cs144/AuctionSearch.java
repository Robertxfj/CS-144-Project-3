package edu.ucla.cs.cs144;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class AuctionSearch implements IAuctionSearch {

	/* 
     * You will probably have to use JDBC to access MySQL data
     * Lucene IndexSearcher class to lookup Lucene index.
     * Read the corresponding tutorial to learn about how to use these.
     *
	 * You may create helper functions or classes to simplify writing these
	 * methods. Make sure that your helper functions are not public,
     * so that they are not exposed to outside of this class.
     *
     * Any new classes that you create should be part of
     * edu.ucla.cs.cs144 package and their source files should be
     * placed at src/edu/ucla/cs/cs144.
     *
     */
    private IndexSearcher _searcher;
    private QueryParser _parser;

    public AuctionSearch() {
        try {
            _searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(new File("/var/lib/lucene/index1"))));
        } catch (IOException ex) {
            System.out.println(ex);
        }
        _parser = new QueryParser("content", new StandardAnalyzer());
    }
	

    
	public SearchResult[] basicSearch(String queryString, int numResultsToSkip,
			int numResultsToReturn) {
		// TODO: Your code here!
        try {
            Query query = _parser.parse(queryString);
            TopDocs topDocs = _searcher.search(query, numResultsToSkip + numResultsToReturn);
            System.out.println(topDocs.totalHits);
            SearchResult[] searchResults = new SearchResult[numResultsToReturn];
            for (int i = numResultsToSkip; i < numResultsToReturn + numResultsToSkip; i++) {
                Document document = _searcher.doc(topDocs.scoreDocs[i].doc);
                searchResults[i] = new SearchResult(document.get("id"), document.get("name"));
            }
            return searchResults;
        } catch (IOException ex) {
            System.out.println(ex);
        } catch (ParseException ex) {
            System.out.println(ex);
        }
        return new SearchResult[0];
	}

	public SearchResult[] spatialSearch(String query, SearchRegion region,
			int numResultsToSkip, int numResultsToReturn) {
		
		SearchResult[] results = basicSearch(query, numResultsToSkip, numResultsToReturn);
		

		SearchResult[] searchResults = new SearchResult[numResultsToReturn];
        
		try {
			Connection connection = DbManager.getConnection(true);
		
			Statement isamStatement = connection.createStatement();
	        ResultSet isamSet = isamStatement.executeQuery(query);
	        
	        
	        int searchResults_index = 0;
	        int found_index = 0;
	        
	        for (SearchResult i:results){
	        	while(isamSet.next()){
	        		if(isamSet.getInt("id") == Integer.parseInt(i.getItemId())) {
	        			if (found_index < numResultsToSkip)
	        				found_index++;
	        			else if(searchResults_index < numResultsToReturn){
	        				searchResults[searchResults_index] = i;
	        				searchResults_index++;
	        			}
	        		}
	        	}
	        }
        } catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
     
		
		return searchResults;
	}

	public String getXMLDataForItemId(String itemId) {
		// TODO: Your code here!
		return "";
	}
	
	public String echo(String message) {
		return message;
	}

}
