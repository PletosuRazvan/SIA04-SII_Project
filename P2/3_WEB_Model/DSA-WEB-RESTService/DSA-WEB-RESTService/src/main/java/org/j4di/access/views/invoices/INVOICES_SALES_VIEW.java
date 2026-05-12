package org.j4di.access.views.invoices;

import java.time.LocalDate;

public interface INVOICES_SALES_VIEW {
    Long getInvoiceId();

    Long getCustomerId();

    String getCustomerName();

    LocalDate getInvoiceDate();

}
