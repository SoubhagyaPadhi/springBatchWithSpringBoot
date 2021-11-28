package com.batch.demo.reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.batch.demo.model.Product;
import com.batch.demo.service.ProductService;

//@Component
public class ProductServiceAdapter {

	Logger logger = LoggerFactory.getLogger(ProductServiceAdapter.class);

	@Autowired
	ProductService prodService;

	public Product nextProduct() throws InterruptedException {

		Product p = null;
		Thread.sleep(1000);
		try {
			p = prodService.getProduct();
			logger.info("Connected to web-service...ok");

		} catch (Exception e) {
			logger.info("exception..." + e.getMessage());
			throw e;
		}
		return p;
	}

}
