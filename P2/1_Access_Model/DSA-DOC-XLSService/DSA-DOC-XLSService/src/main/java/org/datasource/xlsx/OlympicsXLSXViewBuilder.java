package org.datasource.xlsx;

import com.poiji.bind.Poiji;
import com.poiji.option.PoijiOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.logging.Logger;

@Service
public class OlympicsXLSXViewBuilder {
    private static Logger logger = Logger.getLogger(OlympicsXLSXViewBuilder.class.getName());

    @Value("${xlsx.data.source.file.path}")
    private String xlsxFilePath;

    private List<AthleteView> athleteViewList;
    private List<GamesView> gamesViewList;

    public List<AthleteView> getAthleteViewList() { return athleteViewList; }
    public List<GamesView> getGamesViewList() { return gamesViewList; }

    public OlympicsXLSXViewBuilder buildAthletes() throws Exception {
        File xlsxFile = getXlsxFile();
        PoijiOptions options = PoijiOptions.PoijiOptionsBuilder.settings()
                .sheetName("Athletes")
                .build();
        this.athleteViewList = Poiji.fromExcel(xlsxFile, AthleteView.class, options);
        logger.info("Loaded " + athleteViewList.size() + " athletes from XLSX");
        return this;
    }

    public OlympicsXLSXViewBuilder buildGames() throws Exception {
        File xlsxFile = getXlsxFile();
        PoijiOptions options = PoijiOptions.PoijiOptionsBuilder.settings()
                .sheetName("Games")
                .build();
        this.gamesViewList = Poiji.fromExcel(xlsxFile, GamesView.class, options);
        logger.info("Loaded " + gamesViewList.size() + " games from XLSX");
        return this;
    }

    private File getXlsxFile() throws Exception {
        // Copy from classpath to temp file (POIJI needs a File, not InputStream)
        ClassPathResource resource = new ClassPathResource(xlsxFilePath);
        File tempFile = File.createTempFile("olympics_", ".xlsx");
        tempFile.deleteOnExit();
        try (InputStream is = resource.getInputStream()) {
            Files.copy(is, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        return tempFile;
    }
}

