package org.poc;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionManager {

    public Connection getConnection(String txnId) throws SQLException;

    public void close(String txnId) throws SQLException;
}
