package org.example.flink;

public class Event {
  public long order_id;
  public long order_product_id;
  public long order_customer_id;
  public String order_status;

  public Event() {
  }

  public Event(Long order_id, Long order_product_id, Long order_customer_id, String order_status) {
    this.order_id = order_id;
    this.order_customer_id = order_customer_id;
    this.order_product_id = order_product_id;
    this.order_status = order_status;
  }

}