package com.batch.demo;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.batch.demo.model.Prod;
import com.batch.demo.model.Product;

@RestController
public class ProductController {

    @GetMapping("/products")
    public List<Product> getProdcts(){
        ArrayList<Product> products = new ArrayList<>();
        // public Product(Integer productId, String prodName, BigDecimal price, Integer unit, String productDesc)
        products.add(new Product(1,"Apple", "Apple Cell from service", BigDecimal.valueOf(300.00),10));
        products.add(new Product(1,"Dell", "Dell computerfrom service", BigDecimal.valueOf(700.00),10));
        return products;
    }
    
    @GetMapping("/product")
    public Prod getProduct() {
    	
    	return new Prod(1, "Demo Name", BigDecimal.valueOf(500.00), 5, "Demo Description");
    }

}
