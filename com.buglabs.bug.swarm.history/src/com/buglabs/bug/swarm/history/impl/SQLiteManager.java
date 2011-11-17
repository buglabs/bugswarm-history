package com.buglabs.bug.swarm.history.impl;

import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.*;
import com.buglabs.bug.swarm.history.IHistoryManager;
import com.buglabs.bug.swarm.client.ISwarmJsonMessageListener;
import com.buglabs.bug.swarm.client.ISwarmSession;
import com.buglabs.bug.swarm.client.SwarmClientFactory;

public class SQLiteManager implements IHistoryManager {

	private String host;
	private String participationKey;
	private String resourceId;
	private String swarmId;
	private String dbConnectionPath;
	
	private HashMap<String, String> hostMap = new HashMap<String, String>() {{
		put("production", "api.bugswarm.net");
		put("test", "api.test.bugswarm.net");
		put("integration", "api.int.bugswarm.net");
	}};
			
	public SQLiteManager(String server, String participationKey, String resourceId, String swarmId, String dbFilepath) {
		host = hostMap.get(server);
		System.out.println("API Host: " + host);
		this.participationKey = participationKey;
		this.resourceId = resourceId;
		this.swarmId = swarmId;		
		this.dbConnectionPath = getdbConnectionPath(dbFilepath);
	}
	
	@Override
	public void start() throws UnknownHostException, IOException {
		// TODO Auto-generated method stub
		consume();
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

	private void consume() throws UnknownHostException, IOException {
		System.out.println("Starting swarm consumption");
		ISwarmSession swarmSesh = SwarmClientFactory.createSwarmSession(host, participationKey, resourceId, swarmId);
		swarmSesh.addListener(new ISwarmJsonMessageListener() {

			@Override
			public void presenceEvent(String fromSwarm, String fromResource,
					boolean isAvailable) {
				// TODO Auto-generated method stub
				if (fromSwarm.equals(swarmId)) {
					if (isAvailable == true) {
						System.out.println("Now monitoring messages from " + fromResource);
					} else if (isAvailable == false) {
						System.out.println("No longer monitoring messages from " + fromResource);
					}
				}
			}

			@Override
			public void exceptionOccurred(ExceptionType type, String message) {
				// TODO Auto-generated method stub
				System.out.println("Exception of type " + type.toString() + "occurred:");
				System.out.println(message);
			}

			@Override
			public void messageRecieved(Map<String, ?> payload,
					String fromSwarm, String fromResource, boolean isPublic) {
				// TODO Auto-generated method stub
				if (isTableEntryMessage(payload)) {
					System.out.println("Table entry message received:\n" + payload);
					insertTableEntry(payload);
				} else if (isTableUpdateMessage(payload)) {
					System.out.println("Table update message received:\n" + payload);
					insertTableUpdate(payload);
				} else if (isTableQueryMessage(payload)) {
					System.out.println("Table query message received:\n" + payload);
					queryTable(payload);
				}
			}
			
		});
	}
	
	private boolean isTableEntryMessage(Map<String, ?> payload) {
		for (Map.Entry<String, ?> entry : payload.entrySet()) {
			if (entry.getKey().equals("table-entry")) {
				return true;
			}
		}
		return false;
	}
	
	private boolean isTableUpdateMessage(Map<String, ?> payload) {
		for (Map.Entry<String, ?> entry : payload.entrySet()) {
			if (entry.getKey().equals("table-update")) {
				return true;
			}
		}
		return false;
	}
	
	private boolean isTableQueryMessage(Map<String, ?> payload) {
		for (Map.Entry<String, ?> entry : payload.entrySet()) {
			if (entry.getKey().equals("table-query")) {
				return true;
			}
		}
		return false;
	}
	
	private void insertTableEntry(Map<String, ?> payload) {
		
	}
	
	private void insertTableUpdate(Map<String, ?> payload) {
		
	}
	
	private void queryTable(Map<String, ?> payload) {
		
	}
	
	private String getdbConnectionPath(String dbFilepath) {
		String toReturn = "jdbc:sqlite:" + dbFilepath;
		System.out.println("Database Connection Path: " + toReturn);
		return toReturn;
	}
}
