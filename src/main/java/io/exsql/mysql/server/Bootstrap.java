package io.exsql.mysql.server;

import org.apache.spark.sql.classic.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Bootstrap {

    private final static Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);

    public static void main(final String[] args) {
        try (final SparkSession spark = SparkSession
                .builder()
                .appName("mysql-optable-server")
                .master("local[*]")
                .config("spark.driver.host", "127.0.0.1")
                .create()) {

            MysqlServer server = null;
            try {
                server = MysqlServer.create(System.getenv(), spark);
                server.start();
            } catch (final Throwable throwable) {
                LOGGER.error("An error occurred while starting the server. See logs for more information", throwable);
            } finally {
                if (server != null) {
                    server.stop();
                }
            }
        }
    }

}
