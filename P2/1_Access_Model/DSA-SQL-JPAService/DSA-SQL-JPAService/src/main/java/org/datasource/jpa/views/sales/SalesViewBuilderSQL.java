package org.datasource.jpa.views.sales;

import org.datasource.jpa.JPADataSourceConnector;
import org.springframework.stereotype.Service;

import jakarta.persistence.*;
import java.util.ArrayList;
import java.util.List;

@Service
public class SalesViewBuilderSQL {
	
	private String SQL_SALES_SELECT =
            """
                    SELECT i.invoice_Id, i.cust_Id, i.cust_Name, i.invoice_Date, 
                    p.product_Code, p.prod_Name, l.quantity, l.unit_Price 
                    FROM INVOICES i INNER JOIN INVOICE_LINE_ITEMS l ON i.invoice_id = l.invoice_id 
                    INNER JOIN PRODUCTS p ON l.product_code = p.product_code
            """;

	protected List<SalesView> salesViewList = new ArrayList<>();

	public List<SalesView> getSalesViewList() {
		return salesViewList;
	}

	public SalesViewBuilderSQL build(){
		return this.select();
	}

	protected SalesViewBuilderSQL select(){
		EntityManager em = dataSourceConnector.getEntityManager();

		System.out.println("Execute native SQL: " + SQL_SALES_SELECT);
		// Pay attention to the second parameter: SalesViewMapping declared in ProductView Entity
		Query viewQuery = em.createNativeQuery(SQL_SALES_SELECT, "SalesViewMapping");
		this.salesViewList = viewQuery.getResultList();
		
		return this;
	}
	//
	protected JPADataSourceConnector dataSourceConnector;

	public SalesViewBuilderSQL(JPADataSourceConnector dataSourceConnector) {
		this.dataSourceConnector = dataSourceConnector;
	}
	
}