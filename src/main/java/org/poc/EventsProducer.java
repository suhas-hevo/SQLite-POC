package org.poc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class EventsProducer {

    private static final String[] OPERATIONS = {"insert", "update", "delete"};
    private static final Random RANDOM = new Random();

    private static final Logger log = LoggerFactory.getLogger(EventsProducer.class);


    public static void main(String[] args) throws IOException {
        String outputEventsFilePath = args[0];
        if(outputEventsFilePath == null || outputEventsFilePath.isEmpty()){
            throw new RuntimeException("Please provide valid file name as first arg for transaction events output");
        }

        String transactionsConfig = args[1];

        if(transactionsConfig == null){
            throw new RuntimeException("Please provide transactions config for data generation as a Json array as second argument");
        }

        List<Map<String, Integer>> transactionList = extractTransactionsConfig(transactionsConfig);
        log.info("Transactions to be generated: {}", transactionList);

        // Write events to file
        writeEventRowsToFile(transactionList, outputEventsFilePath, Long.parseLong(args[2]));
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

    private static void writeEventRowsToFile(List<Map<String, Integer>> transactionList, String fileName, long eventSize) throws IOException {
        Set<String> started = new HashSet<>();
        Set<String> completed = new HashSet<>();
        long totalSizeBytes = 0;

        Stopwatch stopwatch = Stopwatch.createStarted();

        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(fileName, true))) {
            while(!transactionList.isEmpty()) {
                Map<String, Integer> transactions = transactionList.remove(0);
                // Create LogminerEventRow for each transaction
                log.info("Generating interleaving events for transactions: {}", transactions);
                while (!transactions.isEmpty()) {
                    String transactionId = String.valueOf(transactions.keySet().toArray()[RANDOM.nextInt(transactions.size())]);

                    if (!started.contains(transactionId)) {
                        byte[] startEventBytes = createEventRow(transactionId, "start", eventSize).toByteArray();
                        totalSizeBytes+= startEventBytes.length;
                        dos.writeInt(startEventBytes.length);
                        dos.write(startEventBytes);
                        started.add(transactionId);
                        log.info("Event generation started for transaction: {}", transactionId);
                    }

                    String operation = OPERATIONS[RANDOM.nextInt(OPERATIONS.length)];
                    byte[] eventRowBytes = createEventRow(transactionId, operation, eventSize).toByteArray();
                    totalSizeBytes+=eventRowBytes.length;
                    dos.writeInt(eventRowBytes.length);
                    dos.write(eventRowBytes);

                    transactions.put(transactionId, transactions.get(transactionId) - 1);

                    if(stopwatch.elapsed(TimeUnit.MINUTES) > 2){
                        log.info("Current status of pending events generation: {}", transactions);
                    }

                    if (transactions.get(transactionId) <= 0) {
                        byte[] commitEventBytes = createEventRow(transactionId, "commit", eventSize).toByteArray();
                        totalSizeBytes+= commitEventBytes.length;
                        dos.writeInt(commitEventBytes.length);
                        dos.write(commitEventBytes);
                        transactions.remove(transactionId);
                        completed.add(transactionId);
                        log.info("Event generation finished for transaction: {}", transactionId);
                    }
                }
            }
            log.info("Event generation complete for all transactions, total size: {} Bytes", totalSizeBytes);
        }
        stopwatch.stop();
    }

    private static LogminerEventRowOuterClass.LogminerEventRow createEventRow(String transactionId, String operation, long eventSize) {
        // Create base objects
        String transactionIdPrefix = transactionId;
        LogminerEventRowOuterClass.LogminerDMLEvent logminerDMLEvent = null;

        // For operations "start" and "commit", LogminerDMLEvent is null
        if (!operation.equals("start") && !operation.equals("commit")) {
            logminerDMLEvent = createLogminerDMLEvent(operation, eventSize);
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

    private static LogminerEventRowOuterClass.LogminerDMLEvent createLogminerDMLEvent(String operation, Long eventSize) {
        // Create a sample DMLEntry
        LogminerEventRowOuterClass.DMLEntry.Builder dmlEntryBuilder = LogminerEventRowOuterClass.DMLEntry.newBuilder();

        List<String> values = new ArrayList<>();

        long totalValuesSize = 0;

        // Add more random values to oldValues if needed
        while(totalValuesSize < eventSize) {
            String randomVal = generateRandomValue();
            totalValuesSize+=randomVal.getBytes().length;
            values.add(randomVal);
        }

        if(operation.equals("insert")){
            dmlEntryBuilder.addAllNewValues(values);
        }else if(operation.equals("delete")){
            dmlEntryBuilder.addAllOldValues(values);
        }else{
            dmlEntryBuilder.addAllOldValues(values);
            List<String> newValues = new ArrayList<>(values);
            newValues.remove(values.size()-1);
            newValues.add("random change to value");
            dmlEntryBuilder.addAllNewValues(newValues);
        }

        LogminerEventRowOuterClass.DMLEntry dmlEntry = dmlEntryBuilder
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

    // Generate random integer as a string
    private static String generateRandomInteger() {
        return String.valueOf(RANDOM.nextInt(100000));  // Random integer between 0 and 999
    }

    // Generate random sentence
    private static String generateRandomSentence() {
        String[] words = {"apple", "banana", "cherry", "date", "elephant", "fox"};
        StringBuilder sentence = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            sentence.append(words[RANDOM.nextInt(words.length)]).append(" ");
        }
        return sentence.toString().trim();
    }

    private static String generateRandomDate() {
        LocalDate randomDate = LocalDate.of(2020, 1, 1)
                .plusDays(RANDOM.nextInt(1825));
        return randomDate.toString();
    }

    private static String generateRandomValue() {
        int type = RANDOM.nextInt(3);  // Randomly choose a type (0 = int, 1 = sentence, 2 = date)

        switch (type) {
            case 0: return generateRandomInteger();
            case 1: return generateRandomSentence();
            case 2: return generateRandomDate();
            default: return "random_value";
        }
    }

}
