package com.happy.netty.dev.httpxml.pojo;

public class OrderFactory {

    public static Order create(long orderID) {
        Order order = new Order();
        order.setOrderNumber(orderID);
        order.setTotal(9999.999f);
        Address address = new Address();
        address.setCity("杭州");
        address.setCountry("中国");
        address.setPostCode("300007");
        address.setState("浙江省");
        address.setStreet1("玉古路");
        order.setBillTo(address);
        Customer customer = new Customer();
        customer.setCustomerNumber(orderID);
        customer.setFirstName("sun");
        customer.setLastName("qi");
        order.setCustomer(customer);
        order.setShipping(Shipping.INTERNATIONAL_MAIL);
        order.setShipTo(address);
        return order;
    }
}
