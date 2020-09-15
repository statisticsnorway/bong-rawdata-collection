package no.ssb.dc.bong.commons.postgres;

import com.zaxxer.hikari.HikariDataSource;
import no.ssb.dc.bong.commons.config.SourcePostgresConfiguration;
import no.ssb.dc.bong.commons.jdbc.PostgresDataSource;
import no.ssb.dc.bong.commons.jdbc.PostgresTransactionFactory;
import no.ssb.dc.bong.commons.jdbc.Transaction;
import no.ssb.dc.bong.commons.jdbc.TransactionFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

public class PSQLTest {

    private static final Logger LOG = LoggerFactory.getLogger(PSQLTest.class);

    @Disabled
    @Test
    void initDbAndWrite() throws SQLException {
        SourcePostgresConfiguration psqlConfiguration = new SourcePostgresConfiguration(
                Map.of("database.rawdata.topic", "test-topic")
        );

        try (HikariDataSource dataSource = PostgresDataSource.openPostgresDataSource(psqlConfiguration.asDynamicConfiguration())) {
            PostgresTransactionFactory transactionFactory = new PostgresTransactionFactory(dataSource);
            String topic = psqlConfiguration.asDynamicConfiguration().evaluateToString("rawdata.topic");
            createTopicIfNotExists(transactionFactory, topic, true);
            try (Transaction tx = transactionFactory.createTransaction(false)) {
                PreparedStatement ps = tx.connection().prepareStatement(String.format("INSERT INTO \"%s_bong_item\" (key, value, ts) VALUES (?, ?, ?)", topic));
                {
                    ps.setBytes(1, "/C/A/1".getBytes());
                    ps.setBytes(2, "data".getBytes());
                    ps.setTimestamp(3, Timestamp.from(new Date().toInstant()));
                    ps.addBatch();
                }
                {
                    ps.setBytes(1, "/A/C/2".getBytes());
                    ps.setBytes(2, "data".getBytes());
                    ps.setTimestamp(3, Timestamp.from(new Date().toInstant()));
                    ps.addBatch();
                }
                {
                    ps.setBytes(1, "/B/B/3".getBytes());
                    ps.setBytes(2, "data".getBytes());
                    ps.setTimestamp(3, Timestamp.from(new Date().toInstant()));
                    ps.addBatch();
                }
                {
                    ps.setBytes(1, "/A/B/2".getBytes());
                    ps.setBytes(2, "data".getBytes());
                    ps.setTimestamp(3, Timestamp.from(new Date().toInstant()));
                    ps.addBatch();
                }
                {
                    ps.setBytes(1, "/A/B/1".getBytes());
                    ps.setBytes(2, "data".getBytes());
                    ps.setTimestamp(3, Timestamp.from(new Date().toInstant()));
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            try (Transaction tx = transactionFactory.createTransaction(true)) {
                PreparedStatement ps = tx.connection().prepareStatement(String.format("SELECT key, value, ts FROM \"%s_bong_item\" ORDER BY key", topic));
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    byte[] key = rs.getBytes(1);
                    byte[] value = rs.getBytes(2);
                    Timestamp ts = rs.getTimestamp(3);
                    LOG.trace("{}: {}", new String(key), new String(value));
                }
            }
        }
    }

    void createTopicIfNotExists(TransactionFactory transactionFactory, String topic, boolean dropAndCreateDatabase) {
        if (!transactionFactory.checkIfTableTopicExists(topic, "bong_item") || dropAndCreateDatabase) {
            PostgresDataSource.dropOrCreateDatabase(transactionFactory, topic);
        }
    }

}
