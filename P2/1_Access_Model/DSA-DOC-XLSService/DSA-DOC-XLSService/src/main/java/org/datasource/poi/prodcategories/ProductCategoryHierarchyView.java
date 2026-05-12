package org.datasource.poi.prodcategories;

import com.poiji.annotation.ExcelCell;
import com.poiji.annotation.ExcelSheet;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
@ExcelSheet("CTG_PROD")
public class ProductCategoryHierarchyView {
    @ExcelCell(0)
    private String categoryCode;
    @ExcelCell(1)
    private String categoryName;
    @ExcelCell(2)
    private String parentCategory;
}
