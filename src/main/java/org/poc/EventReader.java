package org.poc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
                    // Process the event (here we just print the details)
                    eventProcessor.handleEvent(eventRow.getTransactionId(), eventRow.getOperation(), eventRow.getLogminerDmlEvent());
                }
            }
        }
    }


    public static LogminerEventRowOuterClass.LogminerEventRow readEventRowFromFile(DataInputStream dis) throws IOException {
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