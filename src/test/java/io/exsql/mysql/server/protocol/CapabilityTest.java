package io.exsql.mysql.server.protocol;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class CapabilityTest {

    @Test
    public void testHasClientProtocol41Flag() {
        int clientFlag = 16777279;
        assertFalse(Capability.hasClientProtocol41Flag(clientFlag),
                   "Client flag 16777279 should have CLIENT_PROTOCOL_41_FLAG set");
    }

    @Test
    public void testHasClientConnectWithDBFlag() {
        int clientFlag = 16777279;
        assertTrue(Capability.hasClientConnectWithDBFlag(clientFlag), 
                   "Client flag 16777279 should have CLIENT_CONNECT_WITH_DB_FLAG set");
    }

    @Test
    public void testHasClientPluginAuthLenencClientDataFlag() {
        int clientFlag = 16777279;
        assertFalse(Capability.hasClientPluginAuthLenencClientDataFlag(clientFlag),
                   "Client flag 16777279 should have CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA set");
    }

}
