package org.datasource.csv;

import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Logger;

@Component
public class CSVDataSourceConnector {
    private static Logger logger = Logger.getLogger(CSVDataSourceConnector.class.getName());

    /**
     * Reads a CSV file from classpath and returns rows as List of Maps (tuple model).
     * First row is treated as header.
     */
    public List<Map<String, String>> readCSV(String resourcePath) throws Exception {
        logger.info("Reading CSV from classpath: " + resourcePath);
        List<Map<String, String>> rows = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new ClassPathResource(resourcePath).getInputStream(), StandardCharsets.UTF_8))) {

            String headerLine = br.readLine();
            if (headerLine == null) return rows;

            String[] headers = headerLine.split(",", -1);

            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",", -1);
                Map<String, String> tuple = new LinkedHashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    String val = (i < values.length) ? values[i].trim() : "";
                    tuple.put(headers[i].trim(), val.isEmpty() ? null : val);
                }
                rows.add(tuple);
            }
        }
        logger.info("CSV loaded: " + rows.size() + " rows from " + resourcePath);
        return rows;
    }
}

