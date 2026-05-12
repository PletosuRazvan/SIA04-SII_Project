package org.datasource.poi.periods;

import com.poiji.bind.Poiji;
import com.poiji.option.PoijiOptions;
import org.datasource.poi.XLSXResourceFileDataSourceConnector;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.List;

/* POIJI Guide
https://github.com/ozlerhakan/poiji?tab=readme-ov-file
 */
//@Service // disabled - using Olympics XLSX builder instead
public class TimePeriodViewBuilder {
    private List<TimePeriodView> viewList;

    public List<TimePeriodView> getViewList() {
        return viewList;
    }

    private XLSXResourceFileDataSourceConnector dataSourceConnector;
    private File xlsxFile;

    public TimePeriodViewBuilder(XLSXResourceFileDataSourceConnector dataSourceConnector) throws Exception {
        this.dataSourceConnector = dataSourceConnector;
        xlsxFile = dataSourceConnector.getXLSXFile();
    }

    // Builder Workflow
    public TimePeriodViewBuilder build() throws Exception{
        PoijiOptions options = PoijiOptions.PoijiOptionsBuilder.settings()
                .datePattern( "dd/MM/yyyy")
                //.headerStart(0)
                //.sheetName( "Periods")
                .build();
        this.viewList = Poiji.fromExcel(this.xlsxFile, TimePeriodView.class, options);
        //
        return this;
    }

}
