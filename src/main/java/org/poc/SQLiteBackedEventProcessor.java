package org.poc;

import org.poc.LogminerEventRowOuterClass.LogminerDMLEvent;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLiteBackedEventProcessor implements EventProcessor{

    private final Map<String, List<LogminerDMLEvent>> transactionsAndEventsMap = new HashMap<>();

    private final TransactionEventsDao transactionEventsDao;

    private static final Logger log = LoggerFactory.getLogger(SQLiteBackedEventProcessor.class);

    private long txnInMemoryLimit;

    private final SQLiteConnectionManager sqLiteConnectionManager = new SQLiteConnectionManager();

    public SQLiteBackedEventProcessor(TransactionEventsDao transactionEventsDao, long txnInMemoryLimit){
        this.txnInMemoryLimit = txnInMemoryLimit;
        this.transactionEventsDao = transactionEventsDao;
    }

    private void handleDMLEvent(String txnId, LogminerDMLEvent event){
        try {
            transactionsAndEventsMap.putIfAbsent(txnId, new ArrayList<>());
            transactionsAndEventsMap.get(txnId).add(event);
            if (transactionsAndEventsMap.get(txnId).size() > txnInMemoryLimit) {
                transactionEventsDao.persistEvents(txnId, transactionsAndEventsMap.remove(txnId));
            }
        }catch (SQLException e){
            log.error("Failed to add events of txn: {} to DB", txnId);
            throw new RuntimeException(e);
        }
    }

    private void handleCommitEvent(String txnId){
        List<LogminerDMLEvent> transactionEvents = new ArrayList<>();
        try {
            transactionEvents.addAll(transactionEventsDao.removeAndGetTransactionEvents(txnId));
            transactionEvents.addAll(transactionsAndEventsMap.getOrDefault(txnId, Collections.emptyList()));
            log.info("Commit event read for transaction: {}, number of events in transaction: {}", txnId, transactionEvents.size());
        }catch (SQLException | InvalidProtocolBufferException e){
            log.error("Error while trying to fetch transaction: {} after reading commit event", txnId, e);
            throw new RuntimeException(e);
        }
    }

    private void handleStartEvent(String txnId){
        log.info("started reading events for transaction: {}", txnId);
        try {
            transactionEventsDao.prepareDBForTxnEvents(txnId);
        }catch (SQLException e){
            log.error("Error while preparing database for transaction: {} by creating table", txnId);
            throw new RuntimeException(e);
        }
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
