package org.spark.service.rest;

public record SQLViewDefinition(
        String viewName,
        String restDataServiceHttpURL,
        String jsonViewSchema,
        String createViewQuery,
        String autoRESTViewPath
) {}