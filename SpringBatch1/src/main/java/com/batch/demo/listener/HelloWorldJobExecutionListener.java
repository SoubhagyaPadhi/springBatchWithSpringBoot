package com.batch.demo.listener;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class HelloWorldJobExecutionListener implements JobExecutionListener {

	@Override
	public void beforeJob(JobExecution jobExecution) {
		System.out.println("Before starting the job with job name - " + jobExecution.getJobInstance().getJobName());
		System.out.println("Before starting the job-" + jobExecution.getExecutionContext().toString());
		jobExecution.getExecutionContext().put("name", "Michael");
		System.out.println("After setting the execution context the job-" + jobExecution.getExecutionContext().toString());

	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		System.out.println("After starting the job - job execution context - " + jobExecution.getExecutionContext());

	}

}
