package org.datasource.poi.custcategories;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor(force = true)
public class CustomerCategoryView {
	private String categoryCode;
	private String categoryName;
	private Double lowerBound;
	private Double upperBound;

	public CustomerCategoryView(String categoryCode, String categoryName, Double lowerBound, Double upperBound) {
		this.categoryCode = categoryCode;
		this.categoryName = categoryName;
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
	}
}
