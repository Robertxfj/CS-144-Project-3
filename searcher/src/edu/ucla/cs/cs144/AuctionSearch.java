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
import java.util.ArrayList;

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
	
    private SearchResult[] search(String queryString) {
    	try {
            Query query = _parser.parse(queryString);
            TopDocs topDocs = _searcher.search(query, 1);
            if (topDocs.totalHits > 0) {
                topDocs = _searcher.search(query, topDocs.totalHits);
            }
            int resultLength = topDocs.totalHits;
            System.out.println(topDocs.totalHits);
            SearchResult[] searchResults = new SearchResult[resultLength];
            for (int j = 0; j < resultLength; j++) {
                Document document = _searcher.doc(topDocs.scoreDocs[j].doc);
                searchResults[j] = new SearchResult(document.get("id"), document.get("name"));
            }
            return searchResults;
        } catch (IOException ex) {
            System.out.println(ex);
        } catch (ParseException ex) {
            System.out.println(ex);
        }
        return new SearchResult[0];
    }
    
	public SearchResult[] basicSearch(String queryString, int numResultsToSkip,
			int numResultsToReturn) {
		// TODO: Your code here!
        try {
            Query query = _parser.parse(queryString);
            TopDocs topDocs = _searcher.search(query, numResultsToSkip + numResultsToReturn);
            int resultLength = Math.max(Math.min(numResultsToReturn, topDocs.totalHits - numResultsToSkip), 0);
            System.out.println(topDocs.totalHits);
            SearchResult[] searchResults = new SearchResult[resultLength];
            for (int i = numResultsToSkip, j = 0; i < numResultsToSkip + resultLength; i++, j++) {
                Document document = _searcher.doc(topDocs.scoreDocs[i].doc);
                searchResults[j] = new SearchResult(document.get("id"), document.get("name"));
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
		
		ArrayList<SearchResult> searchResults = new ArrayList<SearchResult>();
		
		SearchResult[] results = search(query);
        
		try {
			Connection connection = DbManager.getConnection(true);

            String queryString = "SELECT itemId FROM IsamTable WHERE X(location)>=" +
                    region.getLx() + " AND X(location)<=" + region.getRx() +
                    " AND Y(location)>=" + region.getLy() + " AND Y(location)<=" + region.getRy();
			Statement isamStatement = connection.createStatement();
	        ResultSet isamSet = isamStatement.executeQuery(queryString);
	       
	        
	        for (SearchResult i:results){
	        	while(isamSet.next()){
	        		if(isamSet.getInt("ItemId") == Integer.parseInt(i.getItemId())) {
	        			searchResults.add(i);
	        			break;
	        		}
	        	}
	        	isamSet.first();
	        	if (searchResults.size() >= numResultsToSkip+numResultsToReturn)
	        		break;
	        }

            int arraySize = Math.max(Math.min(numResultsToReturn + numResultsToSkip, searchResults.size() - numResultsToSkip), 0);
            SearchResult[] finalResults = new SearchResult[arraySize];
            for (int i = numResultsToSkip; i < arraySize; i++) {
                finalResults[i] = searchResults.get(i);
            }
	        
	        return finalResults;

	        
        } catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
     
		return new SearchResult[0];

	}

	public String getXMLDataForItemId(String itemId) {
		// TODO: Your code here!
		return "";
	}
	
	public String echo(String message) {
		return message;
	}

}
