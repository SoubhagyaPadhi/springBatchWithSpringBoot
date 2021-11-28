package com.batch.demo.listener;

import java.io.FileOutputStream;
import java.io.IOException;

import org.springframework.batch.core.annotation.OnSkipInProcess;
import org.springframework.batch.core.annotation.OnSkipInRead;
import org.springframework.batch.core.annotation.OnSkipInWrite;
import org.springframework.batch.item.file.FlatFileParseException;

public class ProductSkipListener {

	private String fileName = "error/read_skipped";
	private String processErrorFileName = "error/process_skipped";
	private String writeErrorFileName = "error/write_skipped";

	@OnSkipInRead
	public void onSkipRead(Throwable e) throws IOException {

		if (e instanceof FlatFileParseException) {
			FlatFileParseException excep = (FlatFileParseException) e;
			onSkip(excep.getInput(), fileName);
		}
	}

	public void onSkip(Object obj, String fName) throws IOException {
		FileOutputStream outputStream = new FileOutputStream(fName, true);
		outputStream.write(obj.toString().getBytes());
		outputStream.write("\r\n".getBytes());
		outputStream.close();
	}

	@OnSkipInProcess
	public void onSkipProcess(Object obj, Throwable t) throws IOException {

		if (t instanceof RuntimeException) {
			onSkip(obj, processErrorFileName);
		}
	}

	@OnSkipInWrite
	public void onSkipWrite(Object obj, Throwable t) throws IOException {

		if (t instanceof RuntimeException) {
			onSkip(obj, writeErrorFileName);
		}
	}

}
