package org.poc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class EventsProducer {

    private static final String[] OPERATIONS = {"insert", "update", "delete"};
    private static final Random RANDOM = new Random();

    private static final Logger log = LoggerFactory.getLogger(EventsProducer.class);


    public static void main(String[] args) throws IOException {
        String outputEventsFilePath = args[0];
        if(outputEventsFilePath == null || outputEventsFilePath.isEmpty() || !new File(outputEventsFilePath).exists()){
            throw new RuntimeException("Please provide valid file name as first arg for transaction events output");
        }

        String transactionsConfig = args[1];

        if(transactionsConfig == null){
            throw new RuntimeException("Please provide transactions config for data generation as a Json array as second argument");
        }

        List<Map<String, Integer>> transactionList = extractTransactionsConfig(transactionsConfig);
        log.info("Transactions to be generated: {}", transactionList);

        // Write events to file
        writeEventRowsToFile(transactionList, outputEventsFilePath);
    }

    private static List<Map<String, Integer>> extractTransactionsConfig(String transactionsConfig){
        ObjectMapper objectMapper = new ObjectMapper();
        List<Map<String, Integer>> list = new ArrayList<>();

        try {
            // Convert JSON string to List<Map<String, Integer>>
             list = objectMapper.readValue(
                    transactionsConfig,
                    new TypeReference<List<Map<String, Integer>>>() {}
            );
             return list;
        } catch (Exception e) {
            log.error("Error while extracting transactions config: {}", transactionsConfig);
            throw new RuntimeException(e);
        }
    }

    private static void writeEventRowsToFile(List<Map<String, Integer>> transactionList, String fileName) throws IOException {
        Set<String> started = new HashSet<>();
        Set<String> completed = new HashSet<>();

        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(fileName, true))) {
            while(!transactionList.isEmpty()) {
                Map<String, Integer> transactions = transactionList.remove(0);
                // Create LogminerEventRow for each transaction
                log.info("Generating interleaving events for transactions: {}", transactions);
                while (!transactions.isEmpty()) {
                    String transactionId = String.valueOf(transactions.keySet().toArray()[RANDOM.nextInt(transactions.size())]);

                    if (!started.contains(transactionId)) {
                        LogminerEventRowOuterClass.LogminerEventRow startEvent = createEventRow(transactionId, "start");
                        dos.writeInt(startEvent.toByteArray().length);
                        dos.write(startEvent.toByteArray());
                        started.add(transactionId);
                    }

                    String operation = OPERATIONS[RANDOM.nextInt(OPERATIONS.length)];
                    LogminerEventRowOuterClass.LogminerEventRow eventRow = createEventRow(transactionId, operation);
                    dos.writeInt(eventRow.toByteArray().length);
                    dos.write(eventRow.toByteArray());

                    transactions.put(transactionId, transactions.get(transactionId) - 1);
                    if (transactions.get(transactionId) <= 0) {
                        LogminerEventRowOuterClass.LogminerEventRow commitEvent = createEventRow(transactionId, "commit");
                        dos.writeInt(commitEvent.toByteArray().length);
                        dos.write(commitEvent.toByteArray());
                        transactions.remove(transactionId);
                        completed.add(transactionId);
                    }
                    log.info("Event generation complete");
                }
            }
        }
    }

    private static LogminerEventRowOuterClass.LogminerEventRow createEventRow(String transactionId, String operation) {
        // Create base objects
        String transactionIdPrefix = transactionId;
        LogminerEventRowOuterClass.LogminerDMLEvent logminerDMLEvent = null;

        // For operations "start" and "commit", LogminerDMLEvent is null
        if (!operation.equals("start") && !operation.equals("commit")) {
            logminerDMLEvent = createLogminerDMLEvent(operation);
        }

        // Build LogminerEventRow
        LogminerEventRowOuterClass.LogminerEventRow.Builder eventRowBuilder = LogminerEventRowOuterClass.LogminerEventRow.newBuilder()
                .setTransactionId(transactionIdPrefix)
                .setOperation(operation);

        if (logminerDMLEvent != null) {
            eventRowBuilder.setLogminerDmlEvent(logminerDMLEvent);
        }

        return eventRowBuilder.build();
    }

    private static LogminerEventRowOuterClass.LogminerDMLEvent createLogminerDMLEvent(String operation) {
        // Create a sample DMLEntry
        LogminerEventRowOuterClass.DMLEntry dmlEntry = LogminerEventRowOuterClass.DMLEntry.newBuilder()
                .addAllOldValues(List.of("1", "old_value"))
                .addAllNewValues(List.of("1", "new_value"))
                .setEventType(operation.toUpperCase())
                .setObjectOwner("owner")
                .setObjectName("object_name")
                .build();

        // Create the corresponding LogminerDMLEvent
        LogminerEventRowOuterClass.Scn scn = LogminerEventRowOuterClass.Scn.newBuilder().setScn(Math.abs(RANDOM.nextLong())).build();
        LogminerEventRowOuterClass.TableId tableId = LogminerEventRowOuterClass.TableId.newBuilder()
                .setCatalogName("my_catalog")
                .setSchemaName("my_schema")
                .setTableName("my_table")
                .setId("1234")
                .build();

        return LogminerEventRowOuterClass.LogminerDMLEvent.newBuilder()
                .setEventType(operation.toUpperCase())
                .setScn(scn)
                .setTableId(tableId)
                .setRowId("row" + RANDOM.nextInt(1000))
                .setRsId("rs" + RANDOM.nextInt(1000))
                .setChangeTime("2024-11-20T12:00:00")
                .setDmlEntry(dmlEntry)
                .build();
    }

}
