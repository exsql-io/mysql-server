package io.exsql.mysql.server.protocol.server;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Streaming implementation of TextResultsetPacketBuilder that processes Spark Dataset rows
 * without loading the entire result set into memory. This addresses the memory bottleneck
 * in the original TextResultsetPacketStreamBuilder.
 * 
 * This implementation uses Spark's iterator() to stream rows lazily, processing them
 * on-demand as MySQL packets are requested. This allows handling of large result sets
 * without OutOfMemory issues.
 */
public class StreamingTextResultsetPacketBuilder implements Iterator<PacketBuilder> {

    private enum StreamingResultsetPhase {
        COLUMN_COUNT,
        COLUMN_DEFINITION,
        ROW_DATA,
        DONE,
        STOP
    }

    private final int clientFlag;
    private final List<ColumnDefinition> columnDefinitions = new ArrayList<>();
    private final StructType schema;
    private final Iterator<Row> rowIterator;
    
    private StreamingResultsetPhase phase = StreamingResultsetPhase.COLUMN_COUNT;
    private Iterator<PacketBuilder> currentPhaseIterator;
    private boolean rowDataStarted = false;

    private StreamingTextResultsetPacketBuilder(final int clientFlag, final Dataset<Row> dataset) {
        this.clientFlag = clientFlag;
        this.schema = dataset.schema();
        
        // Build column definitions from schema
        for (final StructField field : schema.fields()) {
            this.columnDefinitions.add(ComQueryResponseDatasetHelper.createColumnDefinition(field));
        }
        
        // Get iterator for lazy row processing - this doesn't trigger collection
        this.rowIterator = dataset.toLocalIterator();
    }

    public static StreamingTextResultsetPacketBuilder create(final int clientFlag, final Dataset<Row> dataset) {
        return new StreamingTextResultsetPacketBuilder(clientFlag, dataset);
    }

    @Override
    public boolean hasNext() {
        // Ensure we have a current phase iterator
        prepareCurrentPhaseIterator();
        
        if (currentPhaseIterator != null && currentPhaseIterator.hasNext()) {
            return true;
        }
        
        // Current phase is done, try to transition to next phase
        if (transitionToNextPhase()) {
            prepareCurrentPhaseIterator();
            return currentPhaseIterator != null && currentPhaseIterator.hasNext();
        }
        
        return false;
    }

    @Override
    public PacketBuilder next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more packets available");
        }
        
        return currentPhaseIterator.next();
    }

    private void prepareCurrentPhaseIterator() {
        if (currentPhaseIterator != null) {
            return; // Already prepared
        }
        
        switch (phase) {
            case COLUMN_COUNT -> currentPhaseIterator = createColumnCountIterator();
            case COLUMN_DEFINITION -> currentPhaseIterator = createColumnDefinitionIterator();
            case ROW_DATA -> currentPhaseIterator = createRowDataIterator();
            case DONE -> currentPhaseIterator = createDoneIterator();
            case STOP -> currentPhaseIterator = null;
        }
    }

    private boolean transitionToNextPhase() {
        switch (phase) {
            case COLUMN_COUNT -> {
                phase = columnDefinitions.isEmpty() ? 
                    StreamingResultsetPhase.DONE : StreamingResultsetPhase.COLUMN_DEFINITION;
                currentPhaseIterator = null;
                return true;
            }
            case COLUMN_DEFINITION -> {
                phase = StreamingResultsetPhase.ROW_DATA;
                currentPhaseIterator = null;
                return true;
            }
            case ROW_DATA -> {
                phase = StreamingResultsetPhase.DONE;
                currentPhaseIterator = null;
                return true;
            }
            case DONE -> {
                phase = StreamingResultsetPhase.STOP;
                currentPhaseIterator = null;
                return false;
            }
            case STOP -> {
                return false;
            }
        }
        return false;
    }

    private Iterator<PacketBuilder> createColumnCountIterator() {
        return new Iterator<>() {
            private final PacketBuilder packet = ColumnCountPacketBuilder.create(clientFlag, columnDefinitions.size());
            private boolean consumed = false;

            @Override
            public boolean hasNext() {
                return !consumed;
            }

            @Override
            public PacketBuilder next() {
                if (consumed) {
                    throw new NoSuchElementException();
                }
                consumed = true;
                return packet;
            }
        };
    }

    private Iterator<PacketBuilder> createColumnDefinitionIterator() {
        return ColumnDefinitionPacketStreamBuilder.create(columnDefinitions);
    }

    private Iterator<PacketBuilder> createRowDataIterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return rowIterator.hasNext();
            }

            @Override
            public PacketBuilder next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                
                Row row = rowIterator.next();
                List<String> rowData = ComQueryResponseDatasetHelper.convertRowToStringList(row, schema);
                return new RowDataPacketStreamBuilder.RowDataPacketBuilder(rowData);
            }
        };
    }

    private Iterator<PacketBuilder> createDoneIterator() {
        return new Iterator<>() {
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
                if (consumed) {
                    throw new NoSuchElementException();
                }
                consumed = true;
                return packet;
            }
        };
    }
}