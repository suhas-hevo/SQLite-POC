package org.poc.dao;

import com.google.protobuf.InvalidProtocolBufferException;
import org.poc.ConnectionManager;
import org.poc.LogminerEventRowOuterClass.LogminerDMLEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class TransactionEventsBinaryDaoImpl implements TransactionEventsDao{

    private static final Logger log = LoggerFactory.getLogger(TransactionEventsBinaryDaoImpl.class);
    private ConnectionManager connectionManager;
    public TransactionEventsBinaryDaoImpl(ConnectionManager connectionManager){
        this.connectionManager = connectionManager;
    }

    @Override
    public void persistEvents(String txnId, List<LogminerDMLEvent> events) throws SQLException {
        String insertQuery = "INSERT INTO DMLeventStorageBin (event_data) VALUES (?)";

        Connection connection = connectionManager.getConnection(txnId);
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement(insertQuery)) {

            long serTime = 0;
            long serSt = System.currentTimeMillis();
            for (LogminerDMLEvent event : events) {
                // Serialize each DMLevent to a byte array
                long st = System.currentTimeMillis();
                byte[] serializedData = event.toByteArray();
                serTime += (System.currentTimeMillis() - st);
                statement.setBytes(1, serializedData);
                statement.addBatch();
            }
            long serEt = System.currentTimeMillis();
            statement.executeBatch(); // Execute all insertions at once
            connection.commit();
        }finally {
            connection.setAutoCommit(true);
        }
    }

    @Override
    public void persistByteEvents(String txnId, List<byte[]> events) throws SQLException {
        String insertQuery = "INSERT INTO DMLeventStorageBin (event_data) VALUES (?)";
        Connection connection = connectionManager.getConnection(txnId);
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement(insertQuery)) {

            long serTime = 0;
            long serSt = System.currentTimeMillis();
            for (byte[] event : events) {
                // Serialize each DMLevent to a byte array
                long st = System.currentTimeMillis();
                serTime += (System.currentTimeMillis() - st);
                statement.setBytes(1, event);
                statement.addBatch();
            }
            long serEt = System.currentTimeMillis();
            statement.executeBatch(); // Execute all insertions at once
            connection.commit();
        }finally {
            connection.setAutoCommit(true);
        }
    }

    @Override
    public void prepareDBForTxnEvents(String txnId) throws SQLException {
        String createTableQuery = """
                CREATE TABLE IF NOT EXISTS DMLeventStorageBin (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_data BLOB NOT NULL
                );
                """;

        Connection connection = connectionManager.getConnection(txnId);
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableQuery);
        }
    }

    @Override
    public Long removeAndGetTransactionEvents(String txnId) throws SQLException, InvalidProtocolBufferException {
        String selectQuery = "SELECT id, event_data FROM DMLeventStorageBin order by id";
        Long eventsCount = 0L;
        long readST = System.currentTimeMillis();
        Connection connection = connectionManager.getConnection(txnId);
        try (PreparedStatement selectStatement = connection.prepareStatement(selectQuery)) {

            long totalSerTime = 0;
            // Fetch all entries
            ResultSet resultSet = selectStatement.executeQuery();
            while (resultSet.next()) {
                byte[] serializedData = resultSet.getBytes("event_data");
                // Deserialize Protobuf data
                long serTm = System.currentTimeMillis();
                LogminerDMLEvent event = LogminerDMLEvent.parseFrom(serializedData);
                totalSerTime += (System.currentTimeMillis() - serTm);
                eventsCount++;
            }
            long et = System.currentTimeMillis();
            log.info("Fetching {} events from db took {}ms and deser took {}",eventsCount, (et-readST), totalSerTime);
            connectionManager.close(txnId);
        }
        return eventsCount;
    }
}
