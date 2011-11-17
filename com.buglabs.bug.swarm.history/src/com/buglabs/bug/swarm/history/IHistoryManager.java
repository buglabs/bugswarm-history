package com.buglabs.bug.swarm.history;

import java.io.IOException;
import java.net.UnknownHostException;

/**
 * Manipulate databases using BUGswarm.
 * 
 * @author barberdt
 *
 */
public interface IHistoryManager {

	/**
	 * Begin consuming the given swarm and perform database queries
	 * based on the consumed messages.
	 */
	public void start() throws UnknownHostException, IOException;
	
	/**
	 * Stop consuming the given swarm and, therefore, stop performing database queries
	 * based on the consumed messages.
	 */
	public void stop();
	
}
