package org.poc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class SQLiteConnectionManager implements ConnectionManager{

    private final Map<String, Connection> connectionMap = new HashMap<>();

    private static final Logger log = LoggerFactory.getLogger(SQLiteBackedEventProcessor.class);

    @Override
    public Connection getConnection(String txnId) throws SQLException {
        Connection connection = connectionMap.get(txnId);
        if(null == connection || connection.isClosed()){
            connectionMap.putIfAbsent(txnId, DriverManager.getConnection("jdbc:sqlite:" + txnId + "sl.db"));
        }
        return connectionMap.get(txnId);
    }

    @Override
    public void close(String txnId) throws SQLException {
        Connection connection = connectionMap.remove(txnId);
        if(connection != null){
            connection.close();
        }

        File dbFile = new File(txnId + ".db");
        if(dbFile.exists()){
            log.info("Deleting DB file for transaction: {}", txnId);
            dbFile.delete();
        }else{
            log.info("No DB file found for transaction: {}", txnId);
        }
    }
}
