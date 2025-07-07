package io.exsql.mysql.server.protocol.server;

import io.netty.buffer.ByteBuf;

import java.util.Iterator;
import java.util.List;

class ColumnDefinitionPacketStreamBuilder implements Iterator<PacketBuilder> {
    private final Iterator<COMQueryResponseBuilder.ColumnDefinition> columnDefinitions;

    ColumnDefinitionPacketStreamBuilder(final Iterator<COMQueryResponseBuilder.ColumnDefinition> columnDefinitions) {
        this.columnDefinitions = columnDefinitions;
    }

    static ColumnDefinitionPacketStreamBuilder create(final List<COMQueryResponseBuilder.ColumnDefinition> columnDefinitions) {
        return new ColumnDefinitionPacketStreamBuilder(columnDefinitions.iterator());
    }

    @Override
    public boolean hasNext() {
        return columnDefinitions.hasNext();
    }

    @Override
    public PacketBuilder next() {
        return new ColumnDefinitionPacketBuilder(columnDefinitions.next());
    }

    private static class ColumnDefinitionPacketBuilder extends PacketBuilder {
        private final COMQueryResponseBuilder.ColumnDefinition columnDefinition;

        private ColumnDefinitionPacketBuilder(final COMQueryResponseBuilder.ColumnDefinition columnDefinition) {
            this.columnDefinition = columnDefinition;
        }

        @Override
        protected int buildPayload(final ByteBuf buffer) {
            return columnDefinition.build(buffer);
        }
    }
}
