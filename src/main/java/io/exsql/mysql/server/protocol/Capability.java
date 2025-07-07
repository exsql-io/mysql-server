package io.exsql.mysql.server.protocol;

public class Capability {

    public final static int CLIENT_LONG_PASSWORD_FLAG = 1;

    public final static int CLIENT_CONNECT_WITH_DB_FLAG = 8;

    public final static int CLIENT_PROTOCOL_41_FLAG = 512;

    public final static int CLIENT_INTERACTIVE_FLAG = 1024;

    public final static int CLIENT_PLUGIN_AUTH_FLAG = 1 << 19;

    public final static int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 1 << 21;

    public final static int CLIENT_DEPRECATE_EOF_FLAG = 1 << 24;

    public final static int CLIENT_OPTIONAL_RESULTSET_METADATA_FLAG = 1 << 25;

    public final static int CLIENT_REMEMBER_OPTIONS_FLAG = 1 << 31;


    public static int buildCapabilitiesFlag() {
        int flag = CLIENT_LONG_PASSWORD_FLAG;
        flag |= CLIENT_CONNECT_WITH_DB_FLAG;
        flag |= CLIENT_PROTOCOL_41_FLAG;
        flag |= CLIENT_INTERACTIVE_FLAG;
        flag |= CLIENT_PLUGIN_AUTH_FLAG;
        flag |= CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
        flag |= CLIENT_DEPRECATE_EOF_FLAG;
        flag |= CLIENT_OPTIONAL_RESULTSET_METADATA_FLAG;
        flag |= CLIENT_REMEMBER_OPTIONS_FLAG;

        return flag;
    }

    public static boolean hasClientProtocol41Flag(final int flag) {
        return (flag & CLIENT_PROTOCOL_41_FLAG) != 0;
    }

    public static boolean hasClientConnectWithDBFlag(final int flag) {
        return (flag & CLIENT_CONNECT_WITH_DB_FLAG) != 0;
    }

    public static boolean hasClientPluginAuthClientDataFlag(final int flag) {
        return (flag & CLIENT_PLUGIN_AUTH_FLAG) != 0;
    }

    public static boolean hasClientDeprecateEOFFlag(final int flag) {
        return (flag & CLIENT_DEPRECATE_EOF_FLAG) != 0;
    }

    public static boolean hasClientOptionalResultsetMetadataFlag(final int flag) {
        return (flag & CLIENT_OPTIONAL_RESULTSET_METADATA_FLAG) != 0;
    }

    public static boolean hasClientPluginAuthLenencClientDataFlag(final int flag) {
        return (flag & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0;
    }

}
