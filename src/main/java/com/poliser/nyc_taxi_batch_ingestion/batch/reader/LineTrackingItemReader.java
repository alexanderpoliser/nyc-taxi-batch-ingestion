package com.poliser.nyc_taxi_batch_ingestion.batch.reader;

import com.poliser.nyc_taxi_batch_ingestion.domain.model.TaxiCsvRow;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemStream;
import org.springframework.batch.infrastructure.item.ItemStreamException;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Wrapper that reads TaxiCsvRow and adds line number to each record.
 * Implements ItemStream to properly handle lifecycle methods (open, update, close).
 */
public class LineTrackingItemReader<T> implements ItemReader<TaxiCsvRow>, ItemStream {

    private final ItemReader<T> delegate;
    private final AtomicLong lineNumber = new AtomicLong(1);

    public LineTrackingItemReader(ItemReader<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        // Reset line counter on each job execution
        lineNumber.set(1);

        if (delegate instanceof ItemStream itemStream) {
            itemStream.open(executionContext);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        if (delegate instanceof ItemStream itemStream) {
            itemStream.update(executionContext);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        if (delegate instanceof ItemStream itemStream) {
            itemStream.close();
        }
    }

    @Override
    public TaxiCsvRow read() throws Exception {
        @SuppressWarnings("unchecked")
        TaxiCsvRow item = (TaxiCsvRow) delegate.read();

        // Return null when file ends - this signals end of reading
        if (item == null) {
            return null;
        }

        // Increment line number only for valid items
        long currentLine = lineNumber.getAndIncrement();

        // Create new record with line number
        return new TaxiCsvRow(
                currentLine,
                item.vendorId(),
                item.pickupDatetime(),
                item.dropoffDatetime(),
                item.passengerCount(),
                item.tripDistance(),
                item.pickupLongitude(),
                item.pickupLatitude(),
                item.rateCodeId(),
                item.storeAndFwdFlag(),
                item.dropoffLongitude(),
                item.dropoffLatitude(),
                item.paymentType(),
                item.fareAmount(),
                item.extra(),
                item.mtaTax(),
                item.tipAmount(),
                item.tollsAmount(),
                item.improvementSurcharge(),
                item.totalAmount()
        );
    }
}
