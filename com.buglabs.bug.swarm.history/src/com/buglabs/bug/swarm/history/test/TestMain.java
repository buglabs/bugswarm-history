package com.buglabs.bug.swarm.history.test;

import java.io.IOException;
import java.net.UnknownHostException;

import com.buglabs.bug.swarm.history.IHistoryManager;
import com.buglabs.bug.swarm.history.impl.SQLiteManager;

public class TestMain {

	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, UnknownHostException, IOException {
		// TODO Auto-generated method stub
		IHistoryManager myHM = new SQLiteManager("test", "92bfe25f4a04f2e0338ded23ae30af1e482a0709", "4bf436fbee96cacedbb087f4cd41da22bf84e470", "3eae4f23d133a9d61c1582ca66cd45191fefcc57", "/home/barberdt/test.db");
		myHM.start();
	}

}
