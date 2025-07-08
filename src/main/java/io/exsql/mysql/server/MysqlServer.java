package io.exsql.mysql.server;

import io.exsql.mysql.server.protocol.SessionManager;
import io.netty.handler.logging.LogLevel;
import org.apache.spark.sql.classic.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;
import reactor.netty.transport.logging.AdvancedByteBufFormat;

import javax.net.ssl.SSLException;
import java.security.cert.CertificateException;
import java.util.Map;

public final class MysqlServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlServer.class);

    public static final String IO_EXSQL_MYSQL_SERVER_HOST_ENVIRONMENT_VARIABLE_NAME = "IO_EXSQL_MYSQL_SERVER_HOST";

    public static final String IO_EXSQL_MYSQL_SERVER_HOST_DEFAULT = "localhost";

    public static final String IO_EXSQL_MYSQL_SERVER_PORT_ENVIRONMENT_VARIABLE_NAME = "IO_EXSQL_MYSQL_SERVER_PORT";

    public static final String IO_EXSQL_MYSQL_SERVER_PORT_DEFAULT = "0";

    private final String host;

    private final int port;

    private final SessionManager sessionManager;

    private DisposableServer server;

    private MysqlServer(final String host, final int port, final SparkSession spark) throws CertificateException, SSLException {
        this.host = host;
        this.port = port;
        this.sessionManager = new SessionManager(spark);
    }

    public void start() {
        this.server = TcpServer
                .create()
                .host(this.host)
                .port(this.port)
                .wiretap(LOGGER.getName(), LogLevel.DEBUG, AdvancedByteBufFormat.HEX_DUMP)
                .doOnConnection(sessionManager::initialize)
                .bindNow();

        LOGGER.info("Starting server on {}:{}", this.host(), this.port());

        this.server.onDispose().block();
    }

    public void stop() {
        if (this.server != null) {
            this.sessionManager.shutdown();
            this.server.disposeNow();
        }
    }

    public String host() {
        return this.server != null ? this.server.host() : this.host;
    }

    public int port() {
        return this.server != null ? this.server.port() : this.port;
    }

    public static MysqlServer create(final Map<String, String> environment, final SparkSession spark) throws CertificateException, SSLException {
        return MysqlServer.create(
                environment.getOrDefault(IO_EXSQL_MYSQL_SERVER_HOST_ENVIRONMENT_VARIABLE_NAME, IO_EXSQL_MYSQL_SERVER_HOST_DEFAULT),
                Integer.parseInt(environment.getOrDefault(IO_EXSQL_MYSQL_SERVER_PORT_ENVIRONMENT_VARIABLE_NAME, IO_EXSQL_MYSQL_SERVER_PORT_DEFAULT)),
                spark
        );
    }

    public static MysqlServer create(final String host, final int port, final SparkSession spark) throws CertificateException, SSLException {
        return new MysqlServer(host, port, spark);
    }

}