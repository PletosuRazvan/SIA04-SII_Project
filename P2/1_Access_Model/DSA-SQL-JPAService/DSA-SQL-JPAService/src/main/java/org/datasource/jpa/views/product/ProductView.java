package org.datasource.jpa.views.product;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;
import org.datasource.jpa.views.sales.SalesView;
import jakarta.persistence.*;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;


/**
 * The persistent class for the PRODUCTS database table.
 * 
 */
@SqlResultSetMapping(
		name = "SalesViewMapping",
		classes = {
				@ConstructorResult(
						columns = {
								@ColumnResult(name = "invoice_Id", type = long.class),
								@ColumnResult(name = "cust_Id", type = long.class),
								@ColumnResult(name = "cust_Name", type = String.class),
								@ColumnResult(name = "invoice_Date", type = Date.class),
								@ColumnResult(name = "product_Code", type = long.class),
								@ColumnResult(name = "prod_Name", type = String.class),
								@ColumnResult(name = "quantity", type = BigDecimal.class),
								@ColumnResult(name = "unit_Price", type = BigDecimal.class)
						},
						targetClass = SalesView.class
				)
		}
)
@Value
@AllArgsConstructor
@NoArgsConstructor(force = true)
@Entity @Table(name="PRODUCTS")
@NamedQuery(name="ProductView.findAll", query="SELECT p FROM ProductView p")
public class ProductView implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="PRODUCT_CODE")
	private Long productCode;

	@Column(name="BASE_PRICE")
	private BigDecimal basePrice;

	@Column(name="PROD_CATEGORY")
	private String prodCategory;

	@Column(name="PROD_NAME")
	private String prodName;
}