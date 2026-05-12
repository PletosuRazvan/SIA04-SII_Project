package org.spark.service.rest;

import com.fasterxml.jackson.annotation.JsonRawValue;

public record SQLResponse(
        String query,
        @JsonRawValue
        String response
) {}
