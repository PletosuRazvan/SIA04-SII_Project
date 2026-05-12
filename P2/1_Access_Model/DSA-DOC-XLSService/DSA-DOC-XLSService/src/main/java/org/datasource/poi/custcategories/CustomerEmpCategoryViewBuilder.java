package org.datasource.poi.custcategories;

import org.datasource.poi.XLSXResourceFileDataSourceConnector;
import org.datasource.poi.XLSXViewBuilder;
import org.springframework.stereotype.Service;

//@Service // disabled - using Olympics XLSX builder instead
public class CustomerEmpCategoryViewBuilder extends XLSXViewBuilder<CustomerCategoryView> {

    public CustomerEmpCategoryViewBuilder(XLSXResourceFileDataSourceConnector dataSourceConnector) throws Exception {
        //
        this.setDataSourceConnector(dataSourceConnector);
        this.setWorksheet("CTG_CUST_EMP");
        this.setRowHeaderNumber(0);
        this.setViewAdapter(tuple ->
            new CustomerCategoryView(
                    tuple.get("Category_code").toString(),
                    tuple.get("Category_name").toString(),
                    (double)tuple.get("Lower_L"),
                    (double)tuple.get("Upper_L")
            )
        );
    }
}
