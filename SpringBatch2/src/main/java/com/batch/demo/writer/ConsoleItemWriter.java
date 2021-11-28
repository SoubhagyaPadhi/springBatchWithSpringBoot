package com.batch.demo.writer;

import java.util.List;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import com.batch.demo.model.Product;

public class ConsoleItemWriter extends AbstractItemStreamItemWriter<Product>{

	@Override
	public void write(List items) throws Exception {
		items.stream().forEach(System.out::println);
		System.out.println("******** writing each chunk ********");
		
	}

	

}

/*
 * public class ConsoleItemWriter extends AbstractItemStreamItemWriter<Product>{
 * 
 * @Override public void write(List<? extends Product> items) throws Exception {
 * items.stream().forEach(System.out::println);
 * System.out.println("******** writing each chunk ********");
 * 
 * }
 * 
 * 
 * 
 * }
 */
