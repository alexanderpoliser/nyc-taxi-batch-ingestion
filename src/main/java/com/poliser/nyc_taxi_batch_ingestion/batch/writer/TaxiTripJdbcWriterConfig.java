package com.poliser.nyc_taxi_batch_ingestion.batch.writer;

import com.poliser.nyc_taxi_batch_ingestion.domain.model.TaxiTripRaw;
import org.springframework.batch.infrastructure.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class TaxiTripJdbcWriterConfig {

    @Bean
    public JdbcBatchItemWriter<TaxiTripRaw> taxiTripWriter(DataSource dataSource) {

        var writer = new JdbcBatchItemWriter<TaxiTripRaw>();
        writer.setDataSource(dataSource);
        writer.setItemSqlParameterSourceProvider(
                new BeanPropertyItemSqlParameterSourceProvider<>()
        );
        writer.setAssertUpdates(false);

        writer.setSql("""
            INSERT INTO ingestion.taxi_trip_raw (
                source_file,
                line_number,
                vendor_id,
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                trip_distance,
                pickup_longitude,
                pickup_latitude,
                dropoff_longitude,
                dropoff_latitude,
                rate_code_id,
                store_and_fwd_flag,
                payment_type,
                fare_amount,
                extra,
                mta_tax,
                tip_amount,
                tolls_amount,
                improvement_surcharge,
                total_amount
            ) VALUES (
                :sourceFile,
                :lineNumber,
                :vendorId,
                :pickupDatetime,
                :dropoffDatetime,
                :passengerCount,
                :tripDistance,
                :pickupLongitude,
                :pickupLatitude,
                :dropoffLongitude,
                :dropoffLatitude,
                :rateCodeId,
                :storeAndFwdFlag,
                :paymentType,
                :fareAmount,
                :extra,
                :mtaTax,
                :tipAmount,
                :tollsAmount,
                :improvementSurcharge,
                :totalAmount
            )
            ON CONFLICT (source_file, line_number) DO NOTHING
        """);

        writer.afterPropertiesSet();
        return writer;
    }
}
