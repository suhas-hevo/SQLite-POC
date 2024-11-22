package org.poc.dao;

import com.google.protobuf.InvalidProtocolBufferException;
import org.poc.LogminerEventRowOuterClass.LogminerDMLEvent;

import java.sql.SQLException;
import java.util.List;

public interface TransactionEventsDao {

    void persistEvents(String txnId, List<LogminerDMLEvent> events) throws SQLException;

    void prepareDBForTxnEvents(String txnId) throws SQLException;

    Long removeAndGetTransactionEvents(String txnId) throws SQLException, InvalidProtocolBufferException;
}
