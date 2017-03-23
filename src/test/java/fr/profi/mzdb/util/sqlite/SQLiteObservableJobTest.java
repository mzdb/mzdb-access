package fr.profi.mzdb.util.sqlite;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteQueue;

import org.junit.Assert;
import org.junit.Test;

import fr.profi.mzdb.util.sqlite.*;

import rx.Subscriber;

public class SQLiteObservableJobTest {

	private static final URL filename_OVEMB150205_12 = SQLiteObservableJobTest.class.getResource("/OVEMB150205_12.raw.0.9.7.mzDB");

	static {
		SQLite4JavaTest.checkSQLite();
	}

	/**
	 * Non regression test date: August 07th 2015
	 *
	 * @throws URISyntaxException
	 * @throws InterruptedException 
	 * @throws SQLiteException
	 * @throws FileNotFoundException
	 * @throws ClassNotFoundException
	 */
	@Test
	public void observableJobExecutionTest() throws URISyntaxException, InterruptedException {

		final File file_OVEMB150205_12 = new File(filename_OVEMB150205_12.toURI());

		// create reader in main thread
		Assert.assertTrue("file does not exist", file_OVEMB150205_12.isFile());
		SQLiteQueue queue = new SQLiteQueue(file_OVEMB150205_12);
		queue.start();
		
		SQLiteObservableJob<Integer> observableJob = new SQLiteObservableJob<Integer>( queue, new SQLiteJobWrapper<Integer>() {
			public Integer job(SQLiteConnection connection) throws Exception {
				
				String query = "SELECT count(*) FROM sqlite_master where type == 'table'";
				System.out.println("Will execute query: " + query);

				SQLiteQuery sqliteQuery = new SQLiteQuery(connection, query, false);
				
				return sqliteQuery.extractSingleInt();
			}
		});
		
		// Create multiple instances of SQLiteObservableJob in separate threads
		int threadCount = 3;
		ArrayList<Thread> subscribersThreads = new ArrayList<Thread>(threadCount);
		for( int i = 1; i <= threadCount; i++ ) {
			Thread t = newSubscriberThread(observableJob, i);
			t.start();
			subscribersThreads.add(t);
		}
		
		// Create an SQLiteObservableRecord in the main thread
		SQLiteObservableRecord observableRecord = new SQLiteObservableRecord(queue,"SELECT * FROM sqlite_master where type == 'table'");
		observableRecord.subscribe(new Subscriber<SQLiteRecord>() {
	        @Override
	        public void onCompleted() {
	            System.out.println("Completed Observable Record.");
	        }

	        @Override
	        public void onError(Throwable throwable) {
	            System.err.println("Whoops: " + throwable.getMessage());
	        }

	        @Override
	        public void onNext(SQLiteRecord record) {
	        	try {
					System.out.println("Table name is: " + record.columnString("name"));
				} catch (SQLiteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

	        }
	    });
		
		for( Thread t : subscribersThreads) {
			t.join();
		}
		
		queue.stop(true);
		queue.join();
	}
	
	protected Thread newSubscriberThread(SQLiteObservableJob<Integer> observableJob, int threadNumber) {
		return new Thread(new Runnable() {

		    public void run() {
		    	
				System.out.println("SQLiteObservableJobTest Thread #"+threadNumber+" has started");
		    	
		    	observableJob.subscribe(new Subscriber<Integer>() {
			        @Override
			        public void onCompleted() {
			            System.out.println("Completed Observable Job.");
			        }

			        @Override
			        public void onError(Throwable throwable) {
			            System.err.println("Whoops: " + throwable.getMessage());
			        }

			        @Override
			        public void onNext(Integer integer) {
			            System.out.println("mzDB file contains " + integer + " tables");
			            Assert.assertEquals( new Integer(32), integer );
			        }
			    });
		    }
		});
	}

}
