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

import javax.annotation.Nullable;
import javax.tools.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;


//Define Custom ClassLoader
public class ByteClassLoader extends ClassLoader {

	private final HashMap<String, byte[]> byteDataMap = new HashMap<>();

	private ByteClassLoader(ClassLoader parent) {
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

	public Class<? extends AbstractController> loadClassFromByteArray(byte[] byteData, String className)
		throws ClassNotFoundException {
		//Load bytes into hashmap
		if (!loadDataInBytes(byteData, className)) {
			throw new ClassNotFoundException("duplicate class definition for class:" + className);
		}
		return (Class<? extends AbstractController>) loadClass(className, false);
	}

	public static ByteClassLoader create(){
		return new ByteClassLoader(ByteClassLoader.class.getClassLoader());
	}

	public static byte[] createJar(String className, byte[] classMetaData) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try (JarOutputStream tempJar = new JarOutputStream(bos)){
			JarEntry entry = new JarEntry(className);
			tempJar.putNextEntry(entry);
			// write classMetaDate to the jar.
			tempJar.write(classMetaData, 0, classMetaData.length);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return bos.toByteArray();
	}

	public static byte[] compileJavaClass(String className, String sourceCode, @Nullable Iterable<String> options) throws IOException {
		JavaFileObject targetSource = new JavaSourceFromString(className, sourceCode);
		ByteArrayJavaFileObject targetClass = new ByteArrayJavaFileObject(className);
		Iterable<? extends JavaFileObject> compilationUnits = Collections.singletonList(targetSource);

		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		ByteArrayFileManager fileManager = new ByteArrayFileManager(
			compiler.getStandardFileManager(null, null, null), targetClass);
		StringWriter sw = new StringWriter();
		JavaCompiler.CompilationTask task = compiler.getTask(sw, fileManager,
			null, options, null, compilationUnits);
		if (!task.call()) {
			fileManager.close();
			throw new IOException("compile with error:"+sw.toString());
		}
		fileManager.close();
		return targetClass.toByteArray();
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

	// wrapper class - not really a 'FileObject', uses in-memory ByteArrayOutputStream 'bos'
	private static class ByteArrayJavaFileObject extends SimpleJavaFileObject {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		public ByteArrayJavaFileObject(String name) {
			// URI trick from SimpleJavaFileObject constructor - it only recognizes
			// ('file:' or 'jar:'); anything else forces the 'openOutputStream' callback for Kind.CLASS
			super(URI.create("string:///" + name.replace('.', '/') + Kind.CLASS.extension),
				Kind.CLASS);
		}
		public byte[] toByteArray() {
			return bos.toByteArray();
		}
		@Override
		public OutputStream openOutputStream() throws IOException {
			return bos;
		}
	}
	// wrapper class - compiler will use this 'FileManager' to manage compiler output
	private static class ByteArrayFileManager extends ForwardingJavaFileManager<JavaFileManager> {
		ByteArrayJavaFileObject byteArrayJavaFileObject;
		ByteArrayFileManager(StandardJavaFileManager standardJavaFileManager,
							 ByteArrayJavaFileObject byteArrayJavaFileObject) {
			super(standardJavaFileManager);
			this.byteArrayJavaFileObject = byteArrayJavaFileObject;
		}
		@Override
		public JavaFileObject getJavaFileForOutput(Location location, String name, JavaFileObject.Kind kind,
												   FileObject sibling) throws IOException {
			return byteArrayJavaFileObject;
		}
	}
}



