package com.batch.demo.processor;

import org.springframework.batch.item.ItemProcessor;

public class InMemItemProcessor implements ItemProcessor<Integer, Integer>{

	@Override
	public Integer process(Integer item) throws Exception {
		
		return Integer.sum(10, item);
	}

}
