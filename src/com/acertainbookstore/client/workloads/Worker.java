/**
 * 
 */
package com.acertainbookstore.client.workloads;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;

import com.acertainbookstore.business.Book;
import com.acertainbookstore.business.BookCopy;
import com.acertainbookstore.business.StockBook;
import com.acertainbookstore.business.CertainBookStore;
import com.acertainbookstore.interfaces.BookStore;
import com.acertainbookstore.interfaces.StockManager;
import com.acertainbookstore.utils.BookStoreException;

/**
 * 
 * Worker represents the workload runner which runs the workloads with
 * parameters using WorkloadConfiguration and then reports the results
 * 
 */
public class Worker implements Callable<WorkerRunResult> {
    private WorkloadConfiguration configuration = null;
    private int numSuccessfulFrequentBookStoreInteraction = 0;
    private int numTotalFrequentBookStoreInteraction = 0;
    
    public Worker(WorkloadConfiguration config) {
	configuration = config;
    }

    /**
     * Run the appropriate interaction while trying to maintain the configured
     * distributions
     * 
     * Updates the counts of total runs and successful runs for customer
     * interaction
     * 
     * @param chooseInteraction
     * @return
     */
    private boolean runInteraction(float chooseInteraction) {
	try {
	    float percentRareStockManagerInteraction = configuration.getPercentRareStockManagerInteraction();
	    float percentFrequentStockManagerInteraction = configuration.getPercentFrequentStockManagerInteraction();

	    if (chooseInteraction < percentRareStockManagerInteraction) {
		runRareStockManagerInteraction();
	    } else if (chooseInteraction < percentRareStockManagerInteraction
		    + percentFrequentStockManagerInteraction) {
		runFrequentStockManagerInteraction();
	    } else {
		numTotalFrequentBookStoreInteraction++;
		runFrequentBookStoreInteraction();
		numSuccessfulFrequentBookStoreInteraction++;
	    }
	} catch (BookStoreException ex) {
	    return false;
	}
	return true;
    }

    /**
     * Run the workloads trying to respect the distributions of the interactions
     * and return result in the end
     */
    public WorkerRunResult call() throws Exception {
	int count = 1;
	long startTimeInNanoSecs = 0;
	long endTimeInNanoSecs = 0;
	int successfulInteractions = 0;
	long timeForRunsInNanoSecs = 0;

	Random rand = new Random();
	float chooseInteraction;

	// Perform the warmup runs
	while (count++ <= configuration.getWarmUpRuns()) {
	    chooseInteraction = rand.nextFloat() * 100f;
	    runInteraction(chooseInteraction);
	}

	count = 1;
	numTotalFrequentBookStoreInteraction = 0;
	numSuccessfulFrequentBookStoreInteraction = 0;

	// Perform the actual runs
	startTimeInNanoSecs = System.nanoTime();
	while (count++ <= configuration.getNumActualRuns()) {
	    chooseInteraction = rand.nextFloat() * 100f;
	    if (runInteraction(chooseInteraction)) {
		successfulInteractions++;
	    }
	}
	endTimeInNanoSecs = System.nanoTime();
	timeForRunsInNanoSecs += (endTimeInNanoSecs - startTimeInNanoSecs);
	return new WorkerRunResult(successfulInteractions, timeForRunsInNanoSecs, configuration.getNumActualRuns(),
		numSuccessfulFrequentBookStoreInteraction, numTotalFrequentBookStoreInteraction);
    }

    /**
     * Runs the new stock acquisition interaction
     * 
     * @throws BookStoreException
     */
    private synchronized  void runRareStockManagerInteraction() throws BookStoreException {
	// TODO: Add code for New Stock Acquisition Interaction
    	
    	 StockManager stockManager =  configuration.getStockManager();
    	 List<StockBook> bookToGet =  stockManager.getBooks();
    	 Set<StockBook> generateBooks = new HashSet<>();
    	 Set<StockBook> bookToAdd = new HashSet<>();
    	 List<Integer> isbnset = new ArrayList<>();
    	 
    	 generateBooks = BookSetGenerator.nextSetOfStockBooks(configuration.getNumBooksToAdd());
    	 
    	 for (StockBook sb : bookToGet){
    		 isbnset.add(sb.getISBN());
    	 }
    	 
    	 boolean isContains = false;
    	 for (StockBook book : generateBooks){
    		 isContains = isbnset.contains(book.getISBN());
    		 if(isContains==false){
    			 bookToAdd.add(book);
    		 }
    	 }

    	 stockManager.addBooks(bookToAdd);
    }

    /**
     * Runs the stock replenishment interaction
     * 
     * @throws BookStoreException
     */
    private synchronized  void runFrequentStockManagerInteraction() throws BookStoreException {
	// TODO: Add code for Stock Replenishment Interaction
    	
    	StockManager stockManager = configuration.getStockManager();
    	List<StockBook> listSortedCopiesBooks = stockManager.getBooks();
    	Set<BookCopy> bookCopiesSet = new HashSet<BookCopy>();
   	    int numOfBooks = configuration.getNumBooksWithLeastCopies(); 

	    StockBook stockbook;

	    // Sort all books according to the number of their copies in an ascending order
		
	    Collections.sort(listSortedCopiesBooks,new Comparator <StockBook>(){

	    	public int compare( StockBook b1,StockBook b2){
	    		String NumCopies1 = String.valueOf(b1.getNumCopies());
	    		String NumCopies2 = String.valueOf(b2.getNumCopies());
	    		return NumCopies1.compareTo( NumCopies2);
	    	}

	    });	
	    
	   // Find numBooks descending indices of books that will be picked.
		
	 			Set<Integer> tobePicked = new HashSet<>();
	 			int rangePicks = listSortedCopiesBooks.size();
	 			if (rangePicks <= numOfBooks) {
	 				
	 				// We need to add all books.
	 				for (int i = 0; i < rangePicks; i++) {
	 					tobePicked.add(i);
	 				}

	 			} else {

	 				// We need to pick top k  books with smallest quantities in stock.

	 				int indexNum = 0;
	 				while (tobePicked.size() < numOfBooks) {
	 					tobePicked.add(indexNum);
	 					indexNum++;
	 				}
	 			}

	 	// Addcopies to the k books with smallest quantities in stock.

	 			for (Integer index : tobePicked) {
	 				stockbook = listSortedCopiesBooks.get(index);
	 				bookCopiesSet.add(new BookCopy(stockbook.getISBN(), configuration.getNumBookCopiesToBuy()));
	 			}
	 			stockManager.addCopies(bookCopiesSet); 	
    }

    /**
     * Runs the customer interaction
     * 
     * @throws BookStoreException
     */
    private synchronized void runFrequentBookStoreInteraction() throws BookStoreException {
	// TODO: Add code for Customer Interaction
    	Random random = new Random();
   	    int numOfPicked = random.nextInt(configuration.getNumEditorPicksToGet())+1;
   	    BookStore storeBook = configuration.getBookStore();
   	    
    	List<Book> bookEditorPicked = storeBook.getEditorPicks(configuration.getNumEditorPicksToGet());
    	List<Book> bookSampled = new ArrayList<Book>();
    	Set<BookCopy> bookToBuy = new  HashSet<BookCopy>(); 
    	
    	Set<Integer> isbnSet = new HashSet<Integer>();
    	
    	BookCopy bookcopy;
    	
    	for (Book book : bookEditorPicked){
    		isbnSet.add(book.getISBN());
    	}
    	
    	// selects the k books with smallest quantities in stock
    	bookSampled = storeBook.getBooks(configuration.getBookSetGenerator().sampleFromSetOfISBNs(isbnSet, numOfPicked));
    	
    	for (Book book : bookSampled){
    		bookcopy = new BookCopy(book.getISBN(), configuration.getNumBookCopiesToBuy());
    		bookToBuy.add(bookcopy);
    	}	
    	storeBook.buyBooks(bookToBuy);   	
    }

}
