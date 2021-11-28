package com.batch.demo.reader;

import java.util.Arrays;
import java.util.List;

import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

public class InMemItemReader extends AbstractItemStreamItemReader<Integer> {

	Integer[] intArray = { 1, 2, 3, 4, 5, 6, 7, 8, 9};
	List<Integer> intList = Arrays.asList(intArray);
	int index = 0;

	@Override
	public Integer read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		Integer returnItem = null;
		if (index < intList.size()) {
			returnItem = intList.get(index);
			index++;
		} else {
			index = 0;
		}
		return returnItem;
	}

}
