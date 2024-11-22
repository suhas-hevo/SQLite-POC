package org.poc;

import com.google.common.base.Stopwatch;
import org.poc.dao.TransactionEventsBinaryDaoImpl;
import org.poc.dao.TransactionEventsDao;
import org.poc.dao.TransactionEventsStringDaoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class EventReader {

    private static final Logger log = LoggerFactory.getLogger(EventReader.class);

    public static void main(String[] args) throws Exception{
        if(args.length < 3){
            throw new RuntimeException("Please provide all required inputs");
        }
        String inputEventsFilePath = args[0];
        if(inputEventsFilePath == null || inputEventsFilePath.isEmpty() || !new File(inputEventsFilePath).exists()){
            throw new RuntimeException("Please provide valid file name for input events");
        }
        if(args[1] == null || args[1].isEmpty()){
            throw new RuntimeException("Please provide transaction in-memory limit as second argument");
        }
        if(args[2] == null || args[2].isEmpty()){
            throw new RuntimeException("Please provide transaction storage type as third argument");
        }

        TransactionEventsDao transactionEventsDao;
        if(args[2].equals("string")){
            transactionEventsDao = new TransactionEventsStringDaoImpl(new SQLiteConnectionManager());
            log.info("Using string store for events");
        }else{
            transactionEventsDao = new TransactionEventsBinaryDaoImpl(new SQLiteConnectionManager());
            log.info("Using binary store for events");
        }

        EventProcessor eventProcessor = new SQLiteBackedEventProcessor(transactionEventsDao, Long.parseLong(args[1]));

        long totalNumberOfEvents = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();

        try (DataInputStream dis = new DataInputStream(new FileInputStream(inputEventsFilePath))) {
            while (dis.available() > 0) {
                // Read the next Protobuf message from the file
                LogminerEventRowOuterClass.LogminerEventRow eventRow = readEventRowFromFile(dis);
                totalNumberOfEvents++;

                if(stopwatch.elapsed(TimeUnit.MINUTES) > 2){
                    stopwatch.reset();
                    stopwatch.start();
                    log.info("Number of events read from file: {}", totalNumberOfEvents);
                }

                if (eventRow != null) {
                    // Process the event
                    eventProcessor.handleEvent(eventRow.getTransactionId(), eventRow.getOperation(), eventRow.getLogminerDmlEvent());
                }
            }
        }finally {
            clearAllDbFiles();
        }
    }

    private static void clearAllDbFiles(){
        File currentDirectory = new File(".");
        File[] files = currentDirectory.listFiles();

        if (files != null) {
            for (File file : files) {
                if (file.isFile() && file.getName().endsWith(".db")) {
                    if (file.delete()) {
                        log.info("Deleted: {}", file.getName());
                    } else {
                        log.warn("Failed to delete: {}", file.getName());
                    }
                }
            }
        } else {
            log.info("No files found in the current directory for cleanup.");
        }
    }

    private static LogminerEventRowOuterClass.LogminerEventRow readEventRowFromFile(DataInputStream dis) throws IOException {
        int size = dis.readInt(); // Read the integer representing the size of the data
        if (size == -1) {
            return null; // End of file
        }

        // Read the serialized EventRow data
        byte[] rowBytes = new byte[size];
        dis.readFully(rowBytes); // Read the full byte array

        return LogminerEventRowOuterClass.LogminerEventRow.parseFrom(rowBytes); // Deserialize the EventRow from the byte array
    }
}