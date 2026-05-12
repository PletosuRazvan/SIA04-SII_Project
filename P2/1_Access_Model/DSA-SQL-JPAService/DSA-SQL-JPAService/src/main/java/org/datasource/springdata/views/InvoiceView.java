package org.datasource.springdata.views;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.persistence.*;
import java.util.Date;

@Entity
@Table(name="INVOICES")
@Data @AllArgsConstructor @NoArgsConstructor
public class InvoiceView {
    @Id
    @Column(name="invoice_id")
    private Long invoiceId;
    @Column(name="cust_id")
    private Long customerId;
    @Column(name="cust_name")
    private String customerName;
    @Column(name="invoice_date")
    private Date invoiceDate;
}
