package com.batch.demo.processor;

import org.springframework.batch.item.ItemProcessor;

import com.batch.demo.model.Product;

@SuppressWarnings("rawtypes")
public class ProductProcessor implements ItemProcessor<Product, Product>{

	@Override
	public Product process(Product item) throws Exception {
		/*if(item.getProductId() == 2) {
			throw new RuntimeException("Id:-2 Exception");
		}
		else {*/
			item.setProductDesc(item.getProductDesc().toUpperCase());
		//}
		return item;
	}

}
