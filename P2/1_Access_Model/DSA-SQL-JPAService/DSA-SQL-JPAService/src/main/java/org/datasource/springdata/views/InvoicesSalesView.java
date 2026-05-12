package org.datasource.springdata.views;

public interface InvoicesSalesView {
    Long getInvoiceId();

    Long getCustomerId();

    String getCustomerName();

    java.util.Date getInvoiceDate();

    Long getProductCode();

    String getProdName();

    java.math.BigDecimal getQuantity();

    java.math.BigDecimal getUnitPrice();
}
