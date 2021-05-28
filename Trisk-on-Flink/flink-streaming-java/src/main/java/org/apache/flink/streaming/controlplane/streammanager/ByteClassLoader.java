/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.controlplane.streammanager;

import org.apache.flink.streaming.controlplane.udm.AbstractController;
import org.apache.flink.util.FileUtils;

import javax.tools.JavaCompiler;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


//Define Custom ClassLoader
public class ByteClassLoader extends ClassLoader {

	private static ByteClassLoader byteClassLoader = new ByteClassLoader(ByteClassLoader.class.getClassLoader());


	private final HashMap<String, byte[]> byteDataMap = new HashMap<>();

	public ByteClassLoader(ClassLoader parent) {
		super(parent);
	}

	public boolean loadDataInBytes(byte[] byteData, String resourcesName) {
		return byteDataMap.putIfAbsent(resourcesName, byteData) == null;
	}

	@Override
	protected synchronized Class<?> loadClass(
		String name, boolean resolve) throws ClassNotFoundException {

		// First, check if the class has already been loaded
		Class<?> c = findLoadedClass(name);

		if (c == null || byteDataMap.containsKey(c.getName())) {
			// check whether the class should go parent-first
			try {
				// check the URLs
				c = findClass(name);
			} catch (ClassNotFoundException e) {
				// let URLClassLoader do it, which will eventually call the parent
				c = super.loadClass(name, resolve);
			}
		}
		if (resolve) {
			resolveClass(c);
		}
		return c;
	}

	@Override
	protected Class<?> findClass(String className) throws ClassNotFoundException {
		if (byteDataMap.isEmpty())
			throw new ClassNotFoundException("byte data is empty");

		String filePath = className.replaceAll("\\.", "/").concat(".class");
		byte[] extractedBytes = byteDataMap.get(className);
		if (extractedBytes == null)
			throw new ClassNotFoundException("Cannot find " + filePath + " in bytes");

		return defineClass(className, extractedBytes, 0, extractedBytes.length);
	}

	public static Class<? extends AbstractController> loadClassFromByteArray(byte[] byteData, String className)
		throws ClassNotFoundException {
		//Load bytes into hashmap
		if (!byteClassLoader.loadDataInBytes(byteData, className)) {
			throw new ClassNotFoundException("duplicate class definition for class:" + className);
		}
		return (Class<? extends AbstractController>) byteClassLoader.loadClass(className, false);
	}

	public static byte[] compileJavaClass(String className, String sourceCode) throws IOException {

		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		List<JavaSourceFromString> unitsToCompile = new ArrayList<JavaSourceFromString>() {{
			add(new ByteClassLoader.JavaSourceFromString(className, sourceCode));
		}};

		StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
		compiler.getTask(null, fileManager, null, null, null, unitsToCompile)
			.call();
		fileManager.close();

		// My question is: is it possible to compile straight to a byte[] array, and avoid the messiness of dealing with File I/O altogether?
		// see https://stackoverflow.com/questions/2130039/javacompiler-from-jdk-1-6-how-to-write-class-bytes-directly-to-byte-array
		String[] classPathName = className.split("\\.");
		String simpleClassFileName = classPathName[classPathName.length - 1] + ".class";
		return FileUtils.readAllBytes(Paths.get(simpleClassFileName));
	}

	/**
	 * A file object used to represent source coming from a string.
	 */
	private static class JavaSourceFromString extends SimpleJavaFileObject {
		/**
		 * The source code of this "file".
		 */
		final String code;

		/**
		 * Constructs a new JavaSourceFromString.
		 *
		 * @param name the name of the compilation unit represented by this file object
		 * @param code the source code for the compilation unit represented by this file object
		 */
		JavaSourceFromString(String name, String code) {
			super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension),
				Kind.SOURCE);
			this.code = code;
		}

		@Override
		public CharSequence getCharContent(boolean ignoreEncodingErrors) {
			return code;
		}
	}
}



