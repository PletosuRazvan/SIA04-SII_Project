package org.datasource.poi.prodcategories;

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
public class ProductCategoryHierarchyViewBuilder {
    private List<ProductCategoryHierarchyView> viewList;

    public List<ProductCategoryHierarchyView> getViewList() {
        return viewList;
    }

    private XLSXResourceFileDataSourceConnector dataSourceConnector;
    private File xlsxFile;

    public ProductCategoryHierarchyViewBuilder(XLSXResourceFileDataSourceConnector dataSourceConnector) throws Exception {
        this.dataSourceConnector = dataSourceConnector;
        xlsxFile = dataSourceConnector.getXLSXFile();
    }

    // Builder Workflow
    public ProductCategoryHierarchyViewBuilder build() throws Exception{
        PoijiOptions options = PoijiOptions.PoijiOptionsBuilder.settings()
                //.datePattern( "dd-MM-yyyy")
                .headerStart(0)
                //.sheetName( "CTG_PROD")
                .build();
        this.viewList = Poiji.fromExcel(this.xlsxFile, ProductCategoryHierarchyView.class, options);
        System.out.println("SELECT: " + this.viewList.size() + " rows processed.");
        //
        return this;
    }

}
