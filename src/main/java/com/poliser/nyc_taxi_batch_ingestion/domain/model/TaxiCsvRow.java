package com.poliser.nyc_taxi_batch_ingestion.domain.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record TaxiCsvRow(
        Long lineNumber,
        Integer vendorId,
        LocalDateTime pickupDatetime,
        LocalDateTime dropoffDatetime,
        Integer passengerCount,
        BigDecimal tripDistance,
        BigDecimal pickupLongitude,
        BigDecimal pickupLatitude,
        Integer rateCodeId,
        String storeAndFwdFlag,
        BigDecimal dropoffLongitude,
        BigDecimal dropoffLatitude,
        Integer paymentType,
        BigDecimal fareAmount,
        BigDecimal extra,
        BigDecimal mtaTax,
        BigDecimal tipAmount,
        BigDecimal tollsAmount,
        BigDecimal improvementSurcharge,
        BigDecimal totalAmount
) {}
