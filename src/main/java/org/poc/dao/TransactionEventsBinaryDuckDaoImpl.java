package org.poc.dao;

import com.google.protobuf.InvalidProtocolBufferException;
import org.poc.ConnectionManager;
import org.poc.LogminerEventRowOuterClass.LogminerDMLEvent;
import org.poc.SQLiteBackedEventProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class TransactionEventsBinaryDuckDaoImpl implements TransactionEventsDao {

    private final ConnectionManager connectionManager;

    private static final Logger log = LoggerFactory.getLogger(TransactionEventsBinaryDuckDaoImpl.class);
    public TransactionEventsBinaryDuckDaoImpl(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public void persistEvents(String txnId, List<LogminerDMLEvent> events) throws SQLException {
        String insertQuery = "INSERT INTO DMLeventStorageBin (event_data) VALUES (?)";

        Connection connection = connectionManager.getConnection(txnId);
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement(insertQuery)) {
            long serTime = 0;
            long serStart = System.currentTimeMillis();

            for (LogminerDMLEvent event : events) {
                // Serialize each DMLevent to a byte array
                long start = System.currentTimeMillis();
                byte[] serializedData = event.toByteArray();
                serTime += (System.currentTimeMillis() - start);
                statement.setBytes(1, serializedData);
//                statement.execute();
                statement.addBatch();
//                statement.executeBatch();
            }
            long serEnd = System.currentTimeMillis();
            statement.executeBatch(); // Execute all insertions at once
            long exBatch = System.currentTimeMillis();
            connection.commit();
//            log.info("Ser tool {}, withAdd Batch {}, executeBatch {}, commit {}",serTime,(serEnd - serStart), (exBatch-serEnd), (System.currentTimeMillis()-exBatch));
        } finally {
            connection.setAutoCommit(true);
//            connection.close(); // Ensure the connection is closed
        }
    }

    @Override
    public void persistByteEvents(String txnId, List<byte[]> events) throws SQLException {
        String insertQuery = "INSERT INTO DMLeventStorageBin (event_data) VALUES (?)";

        Connection connection = connectionManager.getConnection(txnId);
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement(insertQuery)) {
            long serTime = 0;
            long serStart = System.currentTimeMillis();

            for (byte[] event : events) {
                // Serialize each DMLevent to a byte array
                long start = System.currentTimeMillis();
                serTime += (System.currentTimeMillis() - start);
                statement.setBytes(1, event);
//                statement.execute();
                statement.addBatch();
//                statement.executeBatch();
            }
            long serEnd = System.currentTimeMillis();
            statement.executeBatch(); // Execute all insertions at once
            long exBatch = System.currentTimeMillis();
            connection.commit();
//            log.info("Ser tool {}, withAdd Batch {}, executeBatch {}, commit {}",serTime,(serEnd - serStart), (exBatch-serEnd), (System.currentTimeMillis()-exBatch));
        } finally {
            connection.setAutoCommit(true);
//            connection.close(); // Ensure the connection is closed
        }
    }

    @Override
    public void prepareDBForTxnEvents(String txnId) throws SQLException {
        String seqName = "seq_"+txnId;
        String SEQ = "CREATE SEQUENCE IF NOT EXISTS "+ seqName +" START 1;";
        String createTableQuery =  String.format("CREATE TABLE IF NOT EXISTS DMLeventStorageBin (id INTEGER, event_data BLOB NOT NULL);", seqName);
        Connection connection = connectionManager.getConnection(txnId);
        try (Statement statement = connection.createStatement()) {
            statement.execute(SEQ);
            statement.execute(createTableQuery);
        }
    }

    @Override
    public Long removeAndGetTransactionEvents(String txnId) throws SQLException, InvalidProtocolBufferException {
        String selectQuery = "SELECT id, event_data FROM DMLeventStorageBin ORDER BY id";
        Long eventsCount = 0L;

        Connection connection = connectionManager.getConnection(txnId);
        try (PreparedStatement selectStatement = connection.prepareStatement(selectQuery)) {

            // Fetch all entries
            ResultSet resultSet = selectStatement.executeQuery();
            while (resultSet.next()) {
                eventsCount++;
            }

        } finally {
            connection.close(); // Ensure the connection is closed
        }
        return eventsCount;
    }
}
