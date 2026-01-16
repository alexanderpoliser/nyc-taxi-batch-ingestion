package com.poliser.nyc_taxi_batch_ingestion.batch.reader;

import com.poliser.nyc_taxi_batch_ingestion.domain.model.TaxiCsvRow;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemStreamException;
import org.springframework.batch.infrastructure.item.ItemStreamReader;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Wrapper that reads TaxiCsvRow and adds line number to each record.
 * Implements ItemStreamReader for thread-safe usage with SynchronizedItemStreamReader.
 */
public class LineTrackingItemReader<T> implements ItemStreamReader<TaxiCsvRow> {

    private final ItemStreamReader<T> delegate;
    private final AtomicLong lineNumber = new AtomicLong(1);

    public LineTrackingItemReader(ItemStreamReader<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        lineNumber.set(1);
        delegate.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        delegate.update(executionContext);
    }

    @Override
    public void close() throws ItemStreamException {
        delegate.close();
    }

    @Override
    public TaxiCsvRow read() throws Exception {
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
