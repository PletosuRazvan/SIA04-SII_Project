package org.datasource.jpa.views.sales;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;

// JOIN InvoiceView-InvoiceLineItem-Product
// JP.QL: SELECT NEW SaleView() ...
// JP.SQL: NativeQuery !
@Data @AllArgsConstructor
@NoArgsConstructor(force = true)
public class SalesView {
	// from InvoiceView
	private Long invoiceId;
	private Long customerId;
	private String customerName;
	private Date invoiceDate;
	// from InvoiceLineItem and Products
	private Long productCode;
	private String prodName;
	private BigDecimal quantity;
	private BigDecimal unitPrice;
}
/*
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
@NamedNativeQuery(name = "SalesView.findAll",
		resultClass = SalesView.class,
		resultSetMapping ="SalesViewMapping",
		query = "SELECT i.invoice_Id, i.cust_Id, i.cust_Name, i.invoice_Date, \n" +
				"                    p.product_Code, p.prod_Name, l.quantity, l.unit_Price \n" +
				"                    FROM INVOICES i INNER JOIN INVOICE_LINE_ITEMS l ON i.invoice_id = l.invoice_id \n" +
				"                    INNER JOIN PRODUCTS p ON l.product_code = p.product_code"
)
*/