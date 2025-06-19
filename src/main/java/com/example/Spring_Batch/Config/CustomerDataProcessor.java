package com.example.Spring_Batch.Config;

import com.example.Spring_Batch.entity.Customer;
import org.springframework.batch.item.ItemProcessor;

public  class CustomerDataProcessor implements ItemProcessor<Customer,Customer> {


    @Override
    public Customer process(Customer customer) throws Exception {
//
        return customer;
    }
}
