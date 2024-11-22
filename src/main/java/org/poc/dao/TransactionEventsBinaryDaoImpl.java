package org.poc.dao;

import com.google.protobuf.InvalidProtocolBufferException;
import org.poc.ConnectionManager;
import org.poc.LogminerEventRowOuterClass.LogminerDMLEvent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class TransactionEventsBinaryDaoImpl implements TransactionEventsDao{

    private ConnectionManager connectionManager;
    public TransactionEventsBinaryDaoImpl(ConnectionManager connectionManager){
        this.connectionManager = connectionManager;
    }

    @Override
    public void persistEvents(String txnId, List<LogminerDMLEvent> events) throws SQLException {
        String insertQuery = "INSERT INTO DMLeventStorage (event_data) VALUES (?)";

        Connection connection = connectionManager.getConnection(txnId);
        try (PreparedStatement statement = connection.prepareStatement(insertQuery)) {

            for (LogminerDMLEvent event : events) {
                // Serialize each DMLevent to a byte array
                byte[] serializedData = event.toByteArray();
                statement.setBytes(1, serializedData);
                statement.addBatch();
            }
            statement.executeBatch(); // Execute all insertions at once
        }
    }

    @Override
    public void prepareDBForTxnEvents(String txnId) throws SQLException {
        String createTableQuery = """
                CREATE TABLE IF NOT EXISTS DMLeventStorage (
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
        String selectQuery = "SELECT id, event_data FROM DMLeventStorage order by id";
        Long eventsCount = 0L;

        Connection connection = connectionManager.getConnection(txnId);
        try (PreparedStatement selectStatement = connection.prepareStatement(selectQuery)) {

            // Fetch all entries
            ResultSet resultSet = selectStatement.executeQuery();
            while (resultSet.next()) {
                byte[] serializedData = resultSet.getBytes("event_data");

                // Deserialize Protobuf data
                LogminerDMLEvent event = LogminerDMLEvent.parseFrom(serializedData);
                eventsCount++;
            }
            connectionManager.close(txnId);
        }
        return eventsCount;
    }
}
