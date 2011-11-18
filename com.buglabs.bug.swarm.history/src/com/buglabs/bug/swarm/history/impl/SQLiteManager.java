package com.buglabs.bug.swarm.history.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.awt.List;
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
	
	private ISwarmSession swarmSesh = null;
	
	private HashMap<String, String> hostMap = new HashMap<String, String>() {{
		put("production", "api.bugswarm.net");
		put("test", "api.test.bugswarm.net");
		put("integration", "api.int.bugswarm.net");
	}};
			
	public SQLiteManager(String server, String participationKey, String resourceId, String swarmId, String dbFilepath) throws ClassNotFoundException {
		host = hostMap.get(server);
		System.out.println("API Host: " + host);
		this.participationKey = participationKey;
		this.resourceId = resourceId;
		this.swarmId = swarmId;		
		this.dbConnectionPath = getdbConnectionPath(dbFilepath);
		createDriverManager();
	}
	
	@Override
	public void start() throws UnknownHostException, IOException {
		// TODO Auto-generated method stub
		consume();
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		stopConsuming();
	}

	private void consume() throws UnknownHostException, IOException {
		System.out.println("Starting swarm consumption");
		swarmSesh = SwarmClientFactory.createSwarmSession(host, participationKey, resourceId, swarmId);
		swarmSesh.addListener(new ISwarmJsonMessageListener() {

			@Override
			public void presenceEvent(String fromSwarm, String fromResource,
					boolean isAvailable) {
				// TODO Auto-generated method stub
				if (fromSwarm != null && fromSwarm.equals(swarmId)) {
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
					Map<String, ?> tableEntry = (Map<String, ?>) payload.get("table-entry");
					try {
						insertTableEntry(tableEntry);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
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
	
	private void stopConsuming() {		
		swarmSesh.close();
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
	
	private void insertTableEntry(Map<String, ?> tableEntry) throws SQLException {
		Connection conn = opendbConnection();
		String table = (String) tableEntry.get("table");
		Map<String, ?> entry = (Map<String, ?>) tableEntry.get("entry");
		ArrayList<String> columns = new ArrayList<String>();
		ArrayList<String> values = new ArrayList<String>();
		for (Map.Entry<String, ?> currEntry : entry.entrySet()) {
			columns.add((String) currEntry.getKey());
			values.add((String) currEntry.getValue());
		}
		String query = "INSERT INTO \"" + table + "\" (";
		for (int i=0; i<columns.size()-1; i++) {
			query+= "\"" + columns.get(i) + "\", ";
		}
		query += "\"" + columns.get(columns.size()-1) + "\") VALUES (";
		for (int i=0; i<values.size()-1; i++) {
			query += "\"" + values.get(i) + "\", ";
		}
		query += "\"" + values.get(values.size()-1) + "\");";
		System.out.println("Inserting into table " + table);
		System.out.println("Columns: " + columns.toString());					
		System.out.println("Values:" + values.toString());	
		System.out.println(query);
		
		Statement stmt = conn.createStatement();
		stmt.execute(query);
		System.out.println("Entry inserted");
		closedbConnection(conn);
	}
	
	private void insertTableUpdate(Map<String, ?> payload) {
		
	}
	
	private void queryTable(Map<String, ?> payload) {
		
	}
	
	private void createDriverManager() throws ClassNotFoundException {
		String url = "org.sqlite.JDBC";
		System.out.println("Driver Manager URL: " + url);
		Class.forName(url);		
	}
	
	private Connection opendbConnection() throws SQLException {
		Connection conn = DriverManager.getConnection(dbConnectionPath);
		System.out.println("Connection opened");
		return conn;
	}
	
	private void closedbConnection(Connection conn) throws SQLException {
		conn.close();
		System.out.println("Connection closed");
	}
	
	private String getdbConnectionPath(String dbFilepath) {
		String toReturn = "jdbc:sqlite:" + dbFilepath;
		System.out.println("Database Connection Path: " + toReturn);
		return toReturn;
	}
}
