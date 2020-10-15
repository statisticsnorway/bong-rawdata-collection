package no.ssb.dc.collection.api.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import no.ssb.dc.collection.api.config.SourcePostgresConfiguration;
import no.ssb.dc.collection.api.utils.FileAndClasspathReaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

public class PostgresDataSource {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresDataSource.class);

    // https://github.com/brettwooldridge/HikariCP
    public static HikariDataSource openPostgresDataSource(SourcePostgresConfiguration configuration) {
        LOG.info("Configured database: postgres");
        Properties props = new Properties();
        props.setProperty("dataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");
        props.setProperty("dataSource.serverName", configuration.postgresDriverHost());
        props.setProperty("dataSource.portNumber", configuration.postgresDriverPort());
        props.setProperty("dataSource.user", configuration.postgresDriverUser());
        props.setProperty("dataSource.password", configuration.postgresDriverPassword());
        props.setProperty("dataSource.databaseName", configuration.postgresDriverDatabase());
        props.put("dataSource.logWriter", new PrintWriter(System.out));

        HikariConfig config = new HikariConfig(props);
        config.setAutoCommit(false);
        config.setMaximumPoolSize(10);
        HikariDataSource datasource = new HikariDataSource(config);

        return datasource;
    }

    static HikariDataSource openH2DataSource(String jdbcUrl, String username, String password) {
        LOG.info("Configured database: h2");
        Properties props = new Properties();
        props.setProperty("jdbcUrl", jdbcUrl);
        props.setProperty("username", username);
        props.setProperty("password", password);
        props.put("dataSource.logWriter", new PrintWriter(System.out));

        HikariConfig config = new HikariConfig(props);
        config.setAutoCommit(false);
        config.setMaximumPoolSize(10);
        HikariDataSource datasource = new HikariDataSource(config);

        return datasource;
    }

    public static void dropOrCreateDatabase(TransactionFactory transactionFactory, String topic) {
        dropOrCreateDatabase(transactionFactory, topic, "no/ssb/dc/collection/api/postgres/init/init-db.sql");
    }

    static void dropOrCreateDatabase(TransactionFactory transactionFactory, String topic, String sqlResource) {
        try {
            String initSQL = FileAndClasspathReaderUtils.readFileOrClasspathResource(sqlResource);
            Connection conn = transactionFactory.dataSource().getConnection();
            conn.beginRequest();

            String sql = initSQL.replaceAll("TOPIC", topic);
            //LOG.trace("initSQL: {}", sql);
            try (Scanner s = new Scanner(sql)) {
                s.useDelimiter("(;(\r)?\n)|(--\n)");
                try (Statement st = conn.createStatement()) {
                    try {
                        while (s.hasNext()) {
                            String line = s.next();
                            if (line.startsWith("/*!") && line.endsWith("*/")) {
                                int i = line.indexOf(' ');
                                line = line.substring(i + 1, line.length() - " */".length());
                            }

                            if (line.trim().length() > 0) {
                                //LOG.trace("line: {}", line);
                                st.execute(line);
                            }
                        }
                        conn.commit();
                    } finally {
                        st.close();
                    }
                }
            }

            conn.endRequest();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
