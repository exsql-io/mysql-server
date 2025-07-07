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
        DONE
    }

    private final int clientFlag;

    private final List<COMQueryResponseBuilder.ColumnDefinition> columnDefinitions = new ArrayList<>();

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
//        return switch (this.phase) {
//            case COLUMN_COUNT -> !this.columnDefinitions.isEmpty();
//            case COLUMN_DEFINITION -> {
//                if (this.stream == null) {
//                    this.stream = ColumnDefinitionPacketStreamBuilder.create(this.columnDefinitions);
//                }
//
//                var hasNext = this.stream.hasNext();
//                if (!hasNext) {
//                    this.stream = null;
//                }
//
//                yield hasNext || !this.rows.isEmpty();
//            }
//            case ROW_DATA -> {
//                if (this.stream == null) {
//                    //TODO: initialize stream
//                }
//
//                yield this.stream.hasNext();
//            }
//            default -> false;
//        };
    }

    @Override
    public PacketBuilder next() {
//        return switch (this.phase) {
//            case COLUMN_COUNT -> {
//                    var packet = ColumnCountPacketBuilder.create(this.clientFlag, this.columnDefinitions.size());
//                    this.phase = this.columnDefinitions.isEmpty() ?
//                            TextResultsetPacketStreamPhase.DONE : TextResultsetPacketStreamPhase.COLUMN_DEFINITION;
//                    yield packet;
//            }
//            case COLUMN_DEFINITION -> {
//                if (this.stream.hasNext()) {
//                    yield this.stream.next();
//                } else {
//                    this.phase = this.rows.isEmpty() ?
//                            TextResultsetPacketStreamPhase.DONE : TextResultsetPacketStreamPhase.ROW_DATA;
//                    this.stream = null;
//                    yield next();
//                }
//            }
//            case ROW_DATA -> null;
//            case DONE -> null;
//        };
    }

}
