package org.apache.flink.runtime.util.profiling;

import java.io.IOException;
import java.io.OutputStream;

class OutputStreamDecorator {

	private final OutputStream outputStream;

	OutputStreamDecorator(OutputStream outputStream) {
		this.outputStream = outputStream;
	}

	void println(String s) {
		try {
			outputStream.write((s+'\n').getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
