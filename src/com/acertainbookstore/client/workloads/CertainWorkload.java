package com.acertainbookstore.client.workloads;

import java.awt.Font;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.acertainbookstore.business.CertainBookStore;
import com.acertainbookstore.business.ImmutableStockBook;
import com.acertainbookstore.business.StockBook;
import com.acertainbookstore.client.BookStoreHTTPProxy;
import com.acertainbookstore.client.StockManagerHTTPProxy;
import com.acertainbookstore.interfaces.BookStore;
import com.acertainbookstore.interfaces.StockManager;
import com.acertainbookstore.utils.BookStoreException;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartFrame;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.LogarithmicAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.Range;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

/**
 * 
 * CertainWorkload class runs the workloads by different workers concurrently.
 * It configures the environment for the workers using WorkloadConfiguration
 * objects and reports the metrics
 * 
 */
public class CertainWorkload {
	private static int INITIAL_ISBN = 3044561;
	private static int NUM_COPIES = 10;
	private static int numConcurrentWorkloadThreads = 10;

	private static XYSeriesCollection dataSetThroughPut = new XYSeriesCollection();
	private static XYSeriesCollection dataSetLatency = new XYSeriesCollection();
	private static JFreeChart chart;

	public static void main(String[] args) throws Exception {
		String serverAddress = "http://localhost:8081";

    //Initialise the local store
		CertainBookStore store = new CertainBookStore();
    //Run the workers
    List<List<WorkerRunResult>> localResults = runWorkers(store, store);

    //Initialise the remote store
		BookStoreHTTPProxy storeRPC = new BookStoreHTTPProxy(serverAddress);
		StockManagerHTTPProxy managerRPC = new StockManagerHTTPProxy(serverAddress + "/stock");

    //Run the workers
//		List<List<WorkerRunResult>> rpcResults = null;
    List<List<WorkerRunResult>> rpcResults = runWorkers(storeRPC, managerRPC);

    //Remember to stop the server again
    storeRPC.stop();
    managerRPC.stop();

    //Report the results, print them as graph
    reportMetric(localResults,rpcResults);
	}

  private static List<List<WorkerRunResult>> runWorkers(BookStore bookstore, StockManager stockmanager)
  	throws Exception {
		List<List<WorkerRunResult>> totalWorkersRunResults = new ArrayList<>();

		// Generate data in the bookstore before running the workload
		stockmanager.removeAllBooks();
		initializeBookStoreData(stockmanager);

		ExecutorService exec = Executors.newFixedThreadPool(numConcurrentWorkloadThreads);

        //Run experiment for 1..numConcurrentWorkloadThreads threads.
		for (int i = 0; i < numConcurrentWorkloadThreads; i++) {
			List<Future<WorkerRunResult>> runResults = new ArrayList<>();
			List<WorkerRunResult> workerRunResults = new ArrayList<>();
			
			for (int j = 0; j <= i; j++) {
				WorkloadConfiguration config = new WorkloadConfiguration(bookstore, stockmanager);
				Worker workerTask = new Worker(config);
				
				// Keep the futures to wait for the result from the thread
				runResults.add(exec.submit(workerTask));
			}

	    // Get the results from the threads using the futures returned
			for (Future<WorkerRunResult> futureRunResult : runResults) {
				WorkerRunResult runResult = futureRunResult.get(); // blocking call
				workerRunResults.add(runResult);
			}

      //Add the experiment data to the results
			totalWorkersRunResults.add(workerRunResults);
			stockmanager.removeAllBooks();
		}
			
		exec.shutdownNow(); // shutdown the executor
    return totalWorkersRunResults;
  }

	/**
	 * Computes the metrics and prints them
	 * 
	 * @param workerRunResults
	 */
	private static void reportMetric(List<List<WorkerRunResult>> totalWorkersRunResults,List<List<WorkerRunResult>> totalWorkersRunResultsRPC) {
		long totalRunTime = 0;
		double aggThroughPut = 0;
		double interactions,runTimes;

		XYSeries series1 = new XYSeries("ThroughPut");
		XYSeries series2 = new XYSeries("Latency");
		
		XYSeries seriesRPC1 = new XYSeries("RPC ThroughPut");
		XYSeries seriesRPC2 = new XYSeries("RPC Latency");
		
		for (List<WorkerRunResult> workerRunResults : totalWorkersRunResults){
			for (WorkerRunResult runResult : workerRunResults){

				interactions = runResult.getSuccessfulInteractions();		
				runTimes = runResult.getElapsedTimeInNanoSecs();
				aggThroughPut += 1000000000.0*(interactions / runTimes);
				totalRunTime += runResult.getElapsedTimeInNanoSecs();
			}
			double averageTime = totalRunTime/(workerRunResults.size()*1000000000.0);

			series1.add(workerRunResults.size(),aggThroughPut);
			series2.add(workerRunResults.size(),averageTime);

		}
		
		long totalRunTimeRPC = 0;
		double aggThroughPutRPC = 0;
		double interactionsRPC,runTimesRPC;
		
		for (List<WorkerRunResult> workerRunResultsRPC : totalWorkersRunResultsRPC){
			for (WorkerRunResult runResultRPC : workerRunResultsRPC){

				interactionsRPC = runResultRPC.getSuccessfulInteractions();
				runTimesRPC = runResultRPC.getElapsedTimeInNanoSecs();
				aggThroughPutRPC += 1000000000.0*(interactionsRPC/runTimesRPC);
				totalRunTimeRPC += runResultRPC.getElapsedTimeInNanoSecs();
			}
			double averageTimeRPC = totalRunTimeRPC/(workerRunResultsRPC.size()*1000000000.0);
			
			seriesRPC1.add(workerRunResultsRPC.size(),aggThroughPutRPC);
			seriesRPC2.add(workerRunResultsRPC.size(),averageTimeRPC);

		}
		
		dataSetThroughPut.addSeries(series1);
		dataSetThroughPut.addSeries(seriesRPC1);
		dataSetLatency.addSeries(series2);
		dataSetLatency.addSeries(seriesRPC2);

		ChartFrame frameThroughPut = new ChartFrame("Throughput", createLineChart(dataSetThroughPut,"successful interactions per second", "Aggregate Throughput"));
		ChartFrame frameLatency = new ChartFrame("Latency", createLineChart(dataSetLatency,"seconds", "Latency"));
		frameThroughPut.pack();
		frameThroughPut.setVisible(true);
		frameLatency.pack();
		frameLatency.setVisible(true);
	}
	
	private static JFreeChart createLineChart(XYSeriesCollection dataset, String str, String title){
		 // Right y axis for latency
        chart = ChartFactory.createXYLineChart(title, "clients",
                str, dataset, PlotOrientation.VERTICAL, true, true,
                false);

        XYPlot plot = chart.getXYPlot();
        plot.setDomainPannable(true);
        plot.setRangePannable(true);

        LogarithmicAxis yAxis = new LogarithmicAxis(str);
        yAxis.setRange(new Range(0, 125000));
        plot.setRangeAxis(yAxis);

        chart.getLegend().setItemFont(new Font("Courier New", 12, 12));

        return chart;
	 } 
		
	/**
	 * Generate the data in bookstore before the workload interactions are run
   * 
   * @param StockManager
	 */
	private static void initializeBookStoreData(StockManager stockManager) throws BookStoreException {
		StockBook initial_book = new ImmutableStockBook(INITIAL_ISBN, "Harry Potter and JUnit", "JK Unit", (float) 10, NUM_COPIES, 0, 0, 0,
				false);
		Set<StockBook> booksToAdd = new HashSet<>();
		booksToAdd.add(initial_book);
		stockManager.addBooks(booksToAdd);
	}
	
}
