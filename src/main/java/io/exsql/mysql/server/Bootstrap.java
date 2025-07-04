package io.exsql.mysql.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Bootstrap {

    private final static Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);

    public static void main(final String[] args) {
        final MysqlServer server = MysqlServer.create(System.getenv());
        try {
            server.start();
        } catch (final Throwable throwable) {
            LOGGER.error("An error occurred while starting the server. See logs for more information", throwable);
        } finally {
            server.stop();
        }
    }

}
