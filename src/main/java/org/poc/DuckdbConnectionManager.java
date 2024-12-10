package org.poc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class DuckdbConnectionManager implements ConnectionManager{

    private final Map<String, Connection> connectionMap = new HashMap<>();

    private static final Logger log = LoggerFactory.getLogger(DuckdbConnectionManager.class);

    @Override
    public Connection getConnection(String txnId) throws SQLException {
        Connection staticConn = connectionMap.get(txnId);
        if(null == staticConn || staticConn.isClosed()){
            try {
                Class.forName("org.duckdb.DuckDBDriver");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            staticConn = DriverManager.getConnection("jdbc:duckdb:" + txnId+".duckdb");
            if(!staticConn.isClosed()){
                try (Statement stmt = staticConn.createStatement()) {
                    stmt.execute("PRAGMA threads = " + Runtime.getRuntime().availableProcessors());
                    stmt.execute("PRAGMA checkpoint_threshold = '1GB';");
                }
                connectionMap.put(txnId, staticConn);
            }
        }
        return staticConn;
    }

    @Override
    public void close(String txnId) throws SQLException {
        Connection connection = connectionMap.remove(txnId);
        if(connection != null){
            connection.close();
        }

        File dbFile = new File(txnId + ".duckdb");
        if(dbFile.exists()){
            log.info("Deleting DB file for transaction: {}, {}, {}", txnId, dbFile.getAbsolutePath(), dbFile.length());
            dbFile.delete();
        }else{
            log.info("No DB file found for transaction: {}", txnId);
        }
    }
}
