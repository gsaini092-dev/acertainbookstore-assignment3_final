package com.acertainbookstore.client.workloads;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import com.acertainbookstore.business.ImmutableStockBook;
import com.acertainbookstore.business.StockBook;

/**
 * Helper class to generate stockbooks and isbns modelled similar to Random
 * class
 */
public class BookSetGenerator {

	private static int latestStockBookISBN;

	//Configuration for book generation
	private static int initialCopies = 10;
	private static int titleLen = 5;
	private static int authorLen = 5;

	public BookSetGenerator(int latestStockBook) {
		latestStockBookISBN = latestStockBook;
	}

	/**
	 * Returns num randomly selected isbns from the input set
	 * 
	 * @param num
	 * @return
	 */
	public  Set<Integer> sampleFromSetOfISBNs(Set<Integer> isbns, int num) {
		ArrayList<Integer> list = new ArrayList<>();
		if (isbns.size() <= num)
			return isbns;
		else {
			isbns.iterator().forEachRemaining(list::add);
			Collections.shuffle(list);
			return new HashSet<>(list.subList(0,num));
		}
	}

	/**
	 * Return num stock books. For now return an ImmutableStockBook
	 * 
	 * @param num
	 * @return
	 */
	public static Set<StockBook> nextSetOfStockBooks(int num) {
		HashSet<StockBook> bookSet = new HashSet<>();
		for (int i = 0; i < num; i++) {
			latestStockBookISBN++;
			StockBook book = new ImmutableStockBook(latestStockBookISBN,
													getRandomString(titleLen),
													getRandomString(authorLen),
													10,
													initialCopies,
													0,
													0,
													0,
													true);
			bookSet.add(book);
		}
		return bookSet;
	}

	private static String chars = "abcdefghijklmnoprstuvwxyz";

	private static String getRandomString(int length) {
		char[] buf = new char[length];
		for (int i = 0; i < length; i++) {
		    int pick = ThreadLocalRandom.current().nextInt(chars.length());
			buf[i] = chars.charAt(pick);
		}
		return buf.toString();
	}


}
