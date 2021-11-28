package com.batch.demo.listener;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class HelloWorldStepExecutionListener implements StepExecutionListener{

	@Override
	public void beforeStep(StepExecution stepExecution) {
		System.out.println("Before Step Execution - "+ stepExecution.getJobExecution().getExecutionContext());
		System.out.println("Inside step - "+stepExecution.getJobExecution().getJobParameters());
		
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		System.out.println("After Step Execution - "+ stepExecution.getJobExecution().getExecutionContext());
		return null;
	}

}
