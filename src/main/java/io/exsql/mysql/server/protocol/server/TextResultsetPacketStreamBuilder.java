package io.exsql.mysql.server.protocol.server;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TextResultsetPacketStreamBuilder implements Iterator<PacketBuilder> {

    private enum TextResultsetPacketStreamPhase {
        COLUMN_COUNT,
        COLUMN_DEFINITION,
        ROW_DATA,
        DONE,
        STOP
    }

    private final int clientFlag;

    private final List<ColumnDefinition> columnDefinitions = new ArrayList<>();

    private final List<List<String>> rows = new ArrayList<>();

    private TextResultsetPacketStreamPhase phase = TextResultsetPacketStreamPhase.COLUMN_COUNT;

    private Iterator<PacketBuilder> stream;

    private TextResultsetPacketStreamBuilder(final int clientFlag, final Dataset<Row> dataset) {
        this.clientFlag = clientFlag;

        var schema = dataset.schema();
        for (final StructField field: schema.fields()) {
            this.columnDefinitions.add(COMQueryResponseDatasetHelper.createColumnDefinition(field));
        }

        var rows = dataset.collectAsList();
        for (final Row row: rows) {
            this.rows.add(COMQueryResponseDatasetHelper.convertRowToStringList(row, schema));
        }
    }

    public static TextResultsetPacketStreamBuilder create(final int clientFlag, final Dataset<Row> dataset) {
        return new TextResultsetPacketStreamBuilder(clientFlag, dataset);
    }

    @Override
    public boolean hasNext() {
        prepareNext();
        return switch (this.phase) {
            case COLUMN_COUNT -> true;
            case STOP -> false;
            default -> this.stream.hasNext();
        };
    }

    @Override
    public PacketBuilder next() {
        var next = this.stream.next();
        if (!this.stream.hasNext()) {
            transition();
            this.stream = null;
        }

        return next;
    }

    private void transition() {
        switch (this.phase) {
            case COLUMN_COUNT ->
                    this.phase = this.columnDefinitions.isEmpty() ?
                            TextResultsetPacketStreamPhase.DONE : TextResultsetPacketStreamPhase.COLUMN_DEFINITION;
            case COLUMN_DEFINITION ->
                    this.phase = this.rows.isEmpty() ?
                            TextResultsetPacketStreamPhase.DONE : TextResultsetPacketStreamPhase.ROW_DATA;
            case DONE -> this.phase = TextResultsetPacketStreamPhase.STOP;
            default -> this.phase = TextResultsetPacketStreamPhase.DONE;
        }
    }

    private void prepareNext() {
        if (this.stream == null) {
            switch (this.phase) {
                case COLUMN_COUNT -> this.stream = new Iterator<>() {
                    private final PacketBuilder packet = ColumnCountPacketBuilder.create(clientFlag, columnDefinitions.size());
                    private boolean consumed = false;

                    @Override
                    public boolean hasNext() {
                        return !consumed;
                    }

                    @Override
                    public PacketBuilder next() {
                        consumed = true;
                        return packet;
                    }
                };
                case COLUMN_DEFINITION -> this.stream = ColumnDefinitionPacketStreamBuilder.create(this.columnDefinitions);
                case ROW_DATA -> this.stream = RowDataPacketStreamBuilder.create(this.rows);
                case DONE -> this.stream = new Iterator<>() {
                    private final PacketBuilder packet = OkPacketBuilder
                            .create()
                            .withHeader(OkPacketBuilder.EOF_PACKET_HEADER);
                    private boolean consumed = false;

                    @Override
                    public boolean hasNext() {
                        return !consumed;
                    }

                    @Override
                    public PacketBuilder next() {
                        consumed = true;
                        return packet;
                    }
                };
                case STOP -> {}
            }
        }
    }

}
