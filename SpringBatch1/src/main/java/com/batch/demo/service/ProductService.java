package com.batch.demo.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.batch.demo.model.Product;

@Component
public class ProductService {

	public List<Product> getProducts() {

		List<Product> prodList = new ArrayList<>();
		RestTemplate restTemplate = new RestTemplate();
		String httpUrl = "http://localhost:8080/products";
		Product[] products = restTemplate.getForObject(httpUrl, Product[].class);
		for(Product prod : products) {
			prodList.add(prod);
		}
		return prodList;
	}

}
