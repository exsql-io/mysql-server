package io.exsql.mysql.server.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.Connection;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class SessionManager {

    private final static Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);

    private final static int MAX_SESSION_ID = Integer.MAX_VALUE;

    private final Map<Integer, Session> sessions;

    private final ReentrantLock lock = new ReentrantLock(true);

    private int sessionId = 0;

    public SessionManager() {
        this.sessions = new ConcurrentHashMap<>();
    }

    public void initialize(final Connection connection) {
        var id = nextSessionId();

        LOGGER.debug("Initializing session[{}]: {}:{}", id, connection.channel().remoteAddress(), connection.channel().localAddress());

        var session = new Session(id, connection);
        sessions.put(id, session);
        session.initialize();
    }

    public void close(final int id) {
        sessions.remove(id).close();
    }

    public void shutdown() {
        sessions.values().forEach(Session::close);
    }

    private int nextSessionId() {
        lock.lock();
        try {
            if (sessionId == MAX_SESSION_ID) {
                sessionId = 0;
                return sessionId;
            }

            return sessionId++;
        } finally {
            lock.unlock();
        }
    }

}
