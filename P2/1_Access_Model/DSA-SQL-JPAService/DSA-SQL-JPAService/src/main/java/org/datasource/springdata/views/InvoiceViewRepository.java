package org.datasource.springdata.views;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface InvoiceViewRepository extends JpaRepository<InvoiceView, Long> {
    @Query("SELECT o FROM InvoiceView o WHERE o.customerId = :customerId")
    List<InvoiceView> getInvoicesOfCsutomerViewList(@Param("customerId") Long customerId);

    //
    @Query(nativeQuery = true,
            value = """
                    SELECT i.invoice_Id as invoiceId, i.cust_Id as customerId, 
                    i.cust_Name as customerName, i.invoice_Date as invoiceDate, 
                    p.product_Code as productCode, p.prod_Name as prodName, 
                    l.quantity as quantity, l.unit_Price as unitPrice
                    FROM INVOICES i INNER JOIN INVOICE_LINE_ITEMS l ON i.invoice_id = l.invoice_id 
                    INNER JOIN PRODUCTS p ON l.product_code = p.product_code
            """)
    List<InvoicesSalesView> getInvoicesSalesViewList();
}
/*
https://thorben-janssen.com/spring-data-jpa-dto-native-queries/
*/