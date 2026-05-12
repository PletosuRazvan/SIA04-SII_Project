package org.datasource.xlsx;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.util.logging.Logger;

/**
 * Generates OlympicsData.xlsx. Can run standalone or as Spring component.
 */
public class OlympicsXLSXGenerator {
    private static Logger logger = Logger.getLogger(OlympicsXLSXGenerator.class.getName());

    public static void main(String[] args) throws Exception {
        // Standalone: generate directly into src/main/resources/datasource
        String path = args.length > 0 ? args[0] : "target/classes/datasource/OlympicsData.xlsx";
        File f = new File(path);
        f.getParentFile().mkdirs();
        generateXLSX(f);
        System.out.println("Generated: " + f.getAbsolutePath());
    }

    public static void generateXLSX(File outputFile) throws IOException {
        try (Workbook workbook = new XSSFWorkbook()) {
            // ====== Sheet 1: Athletes ======
            Sheet athletesSheet = workbook.createSheet("Athletes");
            String[] athHeaders = {"ID", "Name", "Sex", "Height", "Weight"};
            Object[][] athletes = {
                {1L, "A Dijiang", "M", 180.0, 80.0},
                {2L, "A Lamusi", "M", 170.0, 60.0},
                {3L, "Gunnar Nielsen Aaby", "M", null, null},
                {4L, "Edgar Lindenau Aabye", "M", null, null},
                {5L, "Christine Jacoba Aaftink", "F", 185.0, 82.0},
                {11L, "Michael Fred Phelps II", "M", 193.0, 91.0},
                {12L, "Usain St. Leo Bolt", "M", 195.0, 94.0},
                {13L, "Simone Arianne Biles", "F", 142.0, 47.0},
                {14L, "Nadia Elena Comaneci", "F", 163.0, 45.0},
                {15L, "Carl Lewis", "M", 188.0, 88.0},
                {16L, "Larisa Semyonovna Latynina", "F", 166.0, 54.0},
                {17L, "Paavo Johannes Nurmi", "M", 174.0, 65.0},
                {18L, "Mark Andrew Spitz", "M", 183.0, 73.0},
                {20L, "Jesse Owens", "M", 178.0, 75.0},
                {22L, "Aladar Gerevich", "M", 175.0, 70.0},
                {23L, "Birgit Fischer", "F", 172.0, 68.0},
                {24L, "Sawao Kato", "M", 163.0, 56.0},
                {29L, "Nikolai Andrianov", "M", 165.0, 60.0},
                {30L, "Raymond Clarence Ewry", "M", 185.0, 80.0},
            };
            createSheet(athletesSheet, athHeaders, athletes);

            // ====== Sheet 2: Games ======
            Sheet gamesSheet = workbook.createSheet("Games");
            String[] gamesHeaders = {"Games", "Year", "Season", "City"};
            Object[][] games = {
                {"1896 Summer", 1896, "Summer", "Athina"},
                {"1900 Summer", 1900, "Summer", "Paris"},
                {"1904 Summer", 1904, "Summer", "St. Louis"},
                {"1908 Summer", 1908, "Summer", "London"},
                {"1912 Summer", 1912, "Summer", "Stockholm"},
                {"1920 Summer", 1920, "Summer", "Antwerpen"},
                {"1924 Summer", 1924, "Summer", "Paris"},
                {"1928 Summer", 1928, "Summer", "Amsterdam"},
                {"1932 Summer", 1932, "Summer", "Los Angeles"},
                {"1936 Summer", 1936, "Summer", "Berlin"},
                {"1948 Summer", 1948, "Summer", "London"},
                {"1952 Summer", 1952, "Summer", "Helsinki"},
                {"1956 Summer", 1956, "Summer", "Melbourne"},
                {"1960 Summer", 1960, "Summer", "Roma"},
                {"1964 Summer", 1964, "Summer", "Tokyo"},
                {"1968 Summer", 1968, "Summer", "Mexico City"},
                {"1972 Summer", 1972, "Summer", "Munich"},
                {"1976 Summer", 1976, "Summer", "Montreal"},
                {"1980 Summer", 1980, "Summer", "Moskva"},
                {"1984 Summer", 1984, "Summer", "Los Angeles"},
                {"1988 Summer", 1988, "Summer", "Seoul"},
                {"1992 Summer", 1992, "Summer", "Barcelona"},
                {"1996 Summer", 1996, "Summer", "Atlanta"},
                {"2000 Summer", 2000, "Summer", "Sydney"},
                {"2004 Summer", 2004, "Summer", "Athina"},
                {"2008 Summer", 2008, "Summer", "Beijing"},
                {"2012 Summer", 2012, "Summer", "London"},
                {"2016 Summer", 2016, "Summer", "Rio de Janeiro"},
            };
            createSheet(gamesSheet, gamesHeaders, games);

            try (FileOutputStream fos = new FileOutputStream(outputFile)) {
                workbook.write(fos);
            }
        }
    }

    private static void createSheet(Sheet sheet, String[] headers, Object[][] data) {
        Row headerRow = sheet.createRow(0);
        for (int i = 0; i < headers.length; i++) {
            headerRow.createCell(i).setCellValue(headers[i]);
        }
        for (int r = 0; r < data.length; r++) {
            Row row = sheet.createRow(r + 1);
            for (int c = 0; c < data[r].length; c++) {
                Cell cell = row.createCell(c);
                Object val = data[r][c];
                if (val == null) cell.setBlank();
                else if (val instanceof Long) cell.setCellValue((Long) val);
                else if (val instanceof Integer) cell.setCellValue((Integer) val);
                else if (val instanceof Double) cell.setCellValue((Double) val);
                else cell.setCellValue(val.toString());
            }
        }
    }
}
