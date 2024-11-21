package org.poc;

import org.poc.LogminerEventRowOuterClass.LogminerDMLEvent;

public interface EventProcessor {

    void handleEvent(String txnId, String operation, LogminerDMLEvent event);
}
