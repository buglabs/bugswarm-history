package com.buglabs.bug.swarm.history.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.awt.List;
import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.*;

import com.buglabs.bug.swarm.history.IHistoryManager;
import com.buglabs.bug.swarm.client.ISwarmJsonMessageListener;
import com.buglabs.bug.swarm.client.ISwarmSession;
import com.buglabs.bug.swarm.client.ISwarmStringMessageListener;
import com.buglabs.bug.swarm.client.SwarmClientFactory;

/**
 * An implementation of the IHistoryManager interface that is specific to use with SQLite databases.
 * 
 * @author barberdt
 *
 */
public class SQLiteManager implements IHistoryManager {

	private String host;
	private String participationKey;
	private String resourceId;
	private String swarmId;
	private String dbConnectionPath;
	
	private ISwarmSession swarmSesh = null;
	
	// Server hostname map
	private HashMap<String, String> hostMap = new HashMap<String, String>() {{
		put("production", "api.bugswarm.net");
		put("test", "api.test.bugswarm.net");
		put("integration", "api.int.bugswarm.net");
	}};
			
	/**
	 * Creates an instance of the SQLiteManager and sets the given instance variables.
	 * 
	 * @param server The name of the BUGswarm server to use. Valid types: 'production', 'test', 'integration'.
	 * @param participationKey The participation API key of the user who's swarm will be used.
	 * @param resourceId The id of the resource to associate with this SQLiteManager.
	 * @param swarmId The id of the swarm to be used for managing the database.
	 * @param dbFilepath The local filepath to the SQLite database. Example: '/home/$USERNAME/databases/my_database.db'.
	 * 
	 * @throws ClassNotFoundException
	 */
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
				if (isTableInsertMessage(payload)) {
					System.out.println("Table insert message received:\n" + payload);					
					Map<String, ?> insert = (Map<String, ?>) payload.get("table-insert");
					try {
						tableInsert(insert);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else if (isTableUpdateMessage(payload)) {
					System.out.println("Table update message received:\n" + payload);
					Map<String, ?> update = (Map<String, ?>) payload.get("table-update");
					try {
						tableUpdate(update);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else if (isTableSelectMessage(payload)) {
					System.out.println("Table select message received:\n" + payload);
					Map<String, ?> select = (Map<String, ?>) payload.get("table-select");
					try {
						tableSelect(select);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			
		});
	}
		
	private void stopConsuming() {		
		swarmSesh.close();
	}
	
	private boolean isTableInsertMessage(Map<String, ?> payload) {
		for (Map.Entry<String, ?> entry : payload.entrySet()) {
			if (entry.getKey().equals("table-insert")) {
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
	
	private boolean isTableSelectMessage(Map<String, ?> payload) {
		for (Map.Entry<String, ?> entry : payload.entrySet()) {
			if (entry.getKey().equals("table-select")) {
				return true;
			}
		}
		return false;
	}
	
	private void tableInsert(Map<String, ?> insert) throws SQLException {
		Connection conn = opendbConnection();
		
		String insertTable = (String) insert.get("table");
		
		Map<String, ?> insertEntry = (Map<String, ?>) insert.get("entry");		
		ArrayList<String> columns = new ArrayList<String>();
		ArrayList<String> values = new ArrayList<String>();
		for (Map.Entry<String, ?> currPair : insertEntry.entrySet()) {
			columns.add((String) currPair.getKey());
			values.add((String) currPair.getValue());
		}

		String query = "INSERT INTO \"" + insertTable + "\" (";
		for (int i=0; i<columns.size()-1; i++) {
			query+= "\"" + columns.get(i) + "\", ";
		}
		query += "\"" + columns.get(columns.size()-1) + "\") VALUES (";
		for (int i=0; i<values.size()-1; i++) {
			query += "\"" + values.get(i) + "\", ";
		}
		query += "\"" + values.get(values.size()-1) + "\");";		
		
		System.out.println("Query: " + query);
		Statement stmt = conn.createStatement();
		stmt.execute(query);
		System.out.println("Entry inserted");
		closedbConnection(conn);
	}
	
	private void tableUpdate(Map<String, ?> update) throws SQLException {
		Connection conn = opendbConnection();
		
		String updateTable = (String) update.get("table");
		
		Map<String, ?> updateUpdate = (Map<String, ?>) update.get("update");		
		ArrayList<String> setColumns = new ArrayList<String>();
		ArrayList<String> setValues = new ArrayList<String>();
		for (Map.Entry<String, ?> currPair : updateUpdate.entrySet()) {
			setColumns.add((String) currPair.getKey());
			setValues.add((String) currPair.getValue());
		}
		
		Map<String, ?> updateEntry = (Map<String, ?>) update.get("entry");		
		ArrayList<String> whereColumns = new ArrayList<String>();
		ArrayList<String> whereValues = new ArrayList<String>();
		for (Map.Entry<String, ?> currPair : updateEntry.entrySet()) {
			whereColumns.add((String) currPair.getKey());
			whereValues.add((String) currPair.getValue());
		}				
		
		String query = "UPDATE \"" + updateTable+ "\" SET ";
		for (int i=0; i<setColumns.size()-1; i++) {
			query+= "\"" + setColumns.get(i) + "\"=\"" + setValues.get(i) + "\",";
		}
		query+= "\"" + setColumns.get(setColumns.size()-1) + "\"=\"" + setValues.get(setColumns.size()-1) + "\" WHERE ";
		for (int i=0; i<whereColumns.size()-1; i++) {
			query += "\"" + whereColumns.get(i) + "\"=\"" + whereValues.get(i) + "\" AND ";
		}
		query+= "\"" + whereColumns.get(whereColumns.size()-1) + "\"=\"" + whereValues.get(whereColumns.size()-1) + "\";";
		
		System.out.println("Query: " + query);
		Statement stmt = conn.createStatement();
		stmt.execute(query);
		System.out.println("Entry updated");
		closedbConnection(conn);
	}
		
	
	private void tableSelect(Map<String, ?> select) throws SQLException {
		Connection conn = opendbConnection();
		
		String selectTable = (String) select.get("table"); 
		System.out.println(selectTable);
		
		ArrayList<String> fields = (ArrayList<String>) select.get("select");
		
		Map<String, ?> selectWhere = (Map<String, ?>) select.get("where");
		ArrayList<String> columns = new ArrayList<String>();
		ArrayList<String> values = new ArrayList<String>();
		for (Map.Entry<String, ?> currPair : selectWhere.entrySet()) {
			columns.add((String) currPair.getKey());
			values.add((String) currPair.getValue());
		}
		
		String query = "SELECT ";
		for (int i=0; i<fields.size()-1; i++) {
			query += "\"" + fields.get(i) + "\", ";
		}
		query += "\"" + fields.get(fields.size()-1) + "\" FROM \"" + selectTable + "\" WHERE ";		
		for (int i=0; i<columns.size()-1; i++) {
			query += "\"" + columns.get(i) + "\"=\"" + values.get(i) + "\" AND ";
		}
		query += "\"" + columns.get(columns.size()-1) + "\"=\"" + values.get(columns.size()-1) + "\";";
		
		System.out.println("Query: " + query);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(query);			
		sendResponse(fields, rs);		
		closedbConnection(conn);
	}
	
	private void sendResponse(ArrayList<String> fields, ResultSet rs) throws SQLException {
		while (rs.next()) {			
			for (int i=0; i<fields.size(); i++) {
				String currField = fields.get(i);
				
			}
		}
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
