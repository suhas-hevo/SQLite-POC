package org.poc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class EventReader {

    private static final Logger log = LoggerFactory.getLogger(EventReader.class);

    public static void main(String[] args) throws Exception{
        String inputEventsFilePath = args[0];
        if(inputEventsFilePath == null || inputEventsFilePath.isEmpty() || !new File(inputEventsFilePath).exists()){
            throw new RuntimeException("Please provide valid file name for input events");
        }
        if(args[0] == null){
            throw new RuntimeException("Please provide transaction in-memory limit as second argument");
        }

        EventProcessor eventProcessor = new SQLiteBackedEventProcessor(new TransactionEventsDaoImpl(new SQLiteConnectionManager()), Long.parseLong(args[1]));

        try (DataInputStream dis = new DataInputStream(new FileInputStream(inputEventsFilePath))) {
            while (dis.available() > 0) {
                // Read the next Protobuf message from the file
                LogminerEventRowOuterClass.LogminerEventRow eventRow = readEventRowFromFile(dis);

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
                        log.info("Deleted: {}" + file.getName());
                    } else {
                        log.warn("Failed to delete: {}" + file.getName());
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