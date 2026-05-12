package org.j4di.access.views.invoices;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface INVOICES_VIEW_Repository extends JpaRepository<INVOICES_VIEW, Long> {
    // findAll() is not supported with hive-jdbc
    // JPQL Query
    @Query("SELECT o FROM INVOICES_VIEW o")
    List<INVOICES_VIEW> get_INVOICES_VIEW();

    @Query("SELECT o FROM INVOICES_VIEW o WHERE o.customerId=:customerId")
    List<INVOICES_VIEW> get_INVOICES_VIEW_ByCustomerId(Long customerId);

    // JDBC SQL Query
    @Query(nativeQuery = true,
            value = """
                    SELECT i.invoiceId, i.customerId,
                    i.customerName, i.invoiceDate
                    FROM INVOICES_VIEW i
            """)
    List<INVOICES_SALES_VIEW> get_INVOICES_SALES_VIEW();
}