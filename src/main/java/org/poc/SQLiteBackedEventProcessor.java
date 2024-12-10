package org.poc;

import org.poc.LogminerEventRowOuterClass.LogminerDMLEvent;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;
import org.poc.dao.TransactionEventsBinaryDaoImpl;
import org.poc.dao.TransactionEventsBinaryDuckDaoImpl;
import org.poc.dao.TransactionEventsDao;
import org.poc.dao.TransactionEventsStringDaoImpl;
import org.poc.dao.TransactionEventsStringDuckDaoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLiteBackedEventProcessor implements EventProcessor{

    private final Map<String, List<LogminerDMLEvent>> transactionsAndEventsMap = new HashMap<>();

    private final TransactionEventsDao transactionEventsDao;

    private static final Logger log = LoggerFactory.getLogger(SQLiteBackedEventProcessor.class);

    private long txnInMemoryLimit;

    public SQLiteBackedEventProcessor(TransactionEventsDao transactionEventsDao, long txnInMemoryLimit){
        this.txnInMemoryLimit = txnInMemoryLimit;
        this.transactionEventsDao = transactionEventsDao;
    }

    private void handleDMLEvent(String txnId, LogminerDMLEvent event){
        try {
            transactionsAndEventsMap.putIfAbsent(txnId, new ArrayList<>());
            transactionsAndEventsMap.get(txnId).add(event);
            if (transactionsAndEventsMap.get(txnId).size() > txnInMemoryLimit) {
//                transactionEventsDao.persistEvents(txnId, transactionsAndEventsMap.remove(txnId));
                persistForDbs(txnId, transactionsAndEventsMap.remove(txnId));
            }
        }catch (Exception e){
            log.error("Failed to add events of txn: {} to DB", txnId);
            throw new RuntimeException(e);
        }
    }

    private void persistForDbs(String txnId, List<LogminerDMLEvent> events) {
        Map<String, TransactionEventsDao> daos = Map.of(
//                "SQLite String", new TransactionEventsStringDaoImpl(new SQLiteConnectionManager()),
                "SQLite Binary", new TransactionEventsBinaryDaoImpl(new SQLiteConnectionManager())
//                "DuckDB String", new TransactionEventsStringDuckDaoImpl(new DuckdbConnectionManager()),
//                "DuckDB Binary", new TransactionEventsBinaryDuckDaoImpl(new DuckdbConnectionManager())
        );
        List<byte[]> eventList = new LinkedList<>();
        for(LogminerDMLEvent event: events){
            eventList.add(event.toByteArray());
        }

        daos.forEach((type, dao) -> {
            long startTime = System.currentTimeMillis();
            try {
                dao.persistByteEvents(txnId, eventList);
                long elapsedTime = System.currentTimeMillis() - startTime;
                log.info("\t\t========> {} Type Took {} ms for {} events", type, elapsedTime, events.size());
            } catch (SQLException e) {
                log.error("Error while persisting events for transaction {} with DAO type: {}", txnId, type, e);
                throw new RuntimeException(e);
            }
        });
        eventList.clear();
    }

    private void handleCommitEvent(String txnId){
        Long totalTransactionEvents = 0L;
        try {
            log.info("Detected Commit event read for transaction: {}, starting read from db", txnId);
            totalTransactionEvents += transactionEventsDao.removeAndGetTransactionEvents(txnId);
            totalTransactionEvents += transactionsAndEventsMap.getOrDefault(txnId, Collections.emptyList()).size();
            transactionsAndEventsMap.remove(txnId);
            log.info("Commit event read for transaction: {}, number of events in transaction: {}", txnId, totalTransactionEvents);
        }catch (SQLException | InvalidProtocolBufferException e){
            log.error("Error while trying to fetch transaction: {} after reading commit event", txnId, e);
            throw new RuntimeException(e);
        }
    }

    private void handleStartEvent(String txnId){
        log.info("started reading events for transaction: {}", txnId);
        try {
//            transactionEventsDao.prepareDBForTxnEvents(txnId);
            handleStartForDbs(txnId);
        }catch (Exception e){
            log.error("Error while preparing database for transaction: {} by creating table", txnId);
            throw new RuntimeException(e);
        }
    }

    private void handleStartForDbs(String txnId) {
        Map<String, TransactionEventsDao> daos = Map.of(
                "SQLite String", new TransactionEventsStringDaoImpl(new SQLiteConnectionManager()),
                "SQLite Binary", new TransactionEventsBinaryDaoImpl(new SQLiteConnectionManager()),
                "DuckDB String", new TransactionEventsStringDuckDaoImpl(new DuckdbConnectionManager()),
                "DuckDB Binary", new TransactionEventsBinaryDuckDaoImpl(new DuckdbConnectionManager())
        );

        daos.forEach((type, dao) -> {
            long startTime = System.currentTimeMillis();
            try {
                dao.prepareDBForTxnEvents(txnId);
                long elapsedTime = System.currentTimeMillis() - startTime;
                log.info("{} Type Took {} ms for creating db tables", type, elapsedTime);
            } catch (SQLException e) {
                log.error("Error while persisting events for transaction {} with DAO type: {}", txnId, type, e);
                throw new RuntimeException(e);
            }
        });
    }

    public void handleEvent(String txnId, String operation, LogminerDMLEvent event){
        switch (operation){
            case "start" : handleStartEvent(txnId);
                            break;
            case "insert", "update", "delete":
                handleDMLEvent(txnId, event);
                            break;
            case "commit": handleCommitEvent(txnId);
                            break;
            default: log.warn("Unknown operation of type: {} ", operation);
        }
    }
}
