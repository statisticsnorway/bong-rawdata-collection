package no.ssb.dc.collection.api.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

public class PostgresTransaction implements Transaction {

    final Connection connection;

    public PostgresTransaction(Connection connection) throws SQLException {
        this.connection = connection;
        connection.beginRequest();
    }

    @Override
    public Connection connection() {
        return connection;
    }

    @Override
    public CompletableFuture<Void> commit() throws PersistenceException {
        try {
            return CompletableFuture.completedFuture(null);
        } finally {
            try {
                connection.commit();
                connection.endRequest();
            } catch (SQLException e) {
                throw new PersistenceException(e);
            } finally {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        }
    }

    @Override
    public CompletableFuture<Void> cancel() {
        try {
            return CompletableFuture.completedFuture(null);
        } finally {
            try {
                connection.rollback();
                connection.endRequest();
            } catch (SQLException e) {
                throw new PersistenceException(e);
            } finally {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        }
    }
}
