package com.batch.demo.service;

import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.batch.demo.model.Product;

//@Service
public class ProductService {

	public Product getProduct() {

		RestTemplate restTemplate = new RestTemplate();
		String httpUrl = "http://localhost:8080/product";
		return restTemplate.getForObject(httpUrl, Product.class);
	}

}
