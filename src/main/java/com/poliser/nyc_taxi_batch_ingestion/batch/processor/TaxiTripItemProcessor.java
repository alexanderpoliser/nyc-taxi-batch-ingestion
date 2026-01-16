package com.poliser.nyc_taxi_batch_ingestion.batch.processor;

import com.poliser.nyc_taxi_batch_ingestion.domain.model.TaxiCsvRow;
import com.poliser.nyc_taxi_batch_ingestion.domain.model.TaxiTripRaw;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class TaxiTripItemProcessor implements ItemProcessor<TaxiCsvRow, TaxiTripRaw> {

    private static final BigDecimal MAX_DISTANCE = new BigDecimal("9999999.999");

    private final String sourceFile;

    public TaxiTripItemProcessor(
            @Value("${taxi.ingestion.source-file:data/yellow_tripdata.csv}") String sourceFile
    ) {
        this.sourceFile = sourceFile;
    }

    @Override
    public TaxiTripRaw process(TaxiCsvRow row) {
        validate(row);

        return new TaxiTripRaw(
                sourceFile,
                row.lineNumber(),

                row.vendorId(),
                row.pickupDatetime(),
                row.dropoffDatetime(),
                row.passengerCount(),
                row.tripDistance(),

                row.pickupLongitude(),
                row.pickupLatitude(),
                row.dropoffLongitude(),
                row.dropoffLatitude(),

                row.rateCodeId(),
                row.storeAndFwdFlag(),
                row.paymentType(),

                row.fareAmount(),
                row.extra(),
                row.mtaTax(),
                row.tipAmount(),
                row.tollsAmount(),
                row.improvementSurcharge(),
                row.totalAmount()
        );
    }

    private void validate(TaxiCsvRow row) {

        if (row.pickupDatetime() == null || row.dropoffDatetime() == null) {
            throw new IllegalArgumentException("Pickup or dropoff datetime is null");
        }

        if (row.pickupDatetime().isAfter(row.dropoffDatetime())) {
            throw new IllegalArgumentException("Pickup after dropoff");
        }

        if (row.passengerCount() != null && row.passengerCount() < 0) {
            throw new IllegalArgumentException("Negative passenger count");
        }

        if (row.tripDistance() != null &&
                row.tripDistance().compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Negative trip distance");
        }

        if (row.tripDistance() != null &&
                row.tripDistance().abs().compareTo(MAX_DISTANCE) > 0) {
            throw new IllegalArgumentException("Trip distance exceeds supported precision");
        }
    }
}
