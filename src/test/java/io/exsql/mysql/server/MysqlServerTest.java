package io.exsql.mysql.server;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class MysqlServerTest {
    @Test
    void verifyCreate() {
        var server = MysqlServer.create("localhost", 8080);
        assertNotNull(server);
        assertEquals("localhost", server.host());
        assertEquals(8080, server.port());
    }
}
