/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.compat.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import parquet.Log;

import com.google.code.externalsorting.ExternalSort;

public class Utils {

  private static final Log LOG = Log.getLog(Utils.class);
  
  public static void closeQuietly(Closeable res) {
    try {
      if(res != null) {
        res.close();
      }
    } catch (IOException ioe) {
      LOG.warn("Exception closing reader " + res + ": " + ioe.getMessage());
    }    
  }

  public static File[] getAllOriginalCSVFiles() {
    File baseDir = new File("../testdata/tpch");
    final File[] csvFiles = baseDir.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.endsWith(".csv");
      }
    });
    return csvFiles;
  }

  public static File getParquetVersionedFile(String prefix, String version, boolean failIfNotExist) 
      throws IOException {
    String fileName = prefix + ".java.parquet";
    File parquetFile = new File("../testdata/tpch/"+version, fileName);
    parquetFile.getParentFile().mkdirs();
    if(failIfNotExist && !parquetFile.exists()) {
      throw new IOException("File " + parquetFile.getAbsolutePath() + " does not exist");
    }
    return parquetFile;
  }

  public static File getParquetImpalaFile(String prefix) throws IOException {
    String fileName = prefix + ".impala.parquet";
    File parquetFile = new File("../testdata/tpch", fileName);
    if(!parquetFile.exists()) {
      throw new IOException("File " + fileName + " does not exist");
    }
    return parquetFile;
  }

  public static String getFileNamePrefix(File file) {
    return file.getName().substring(0, file.getName().indexOf("."));
  }

  public static File getParquetTestFile(String name, String module, boolean deleteIfExists) {
    File outputFile = new File("target/test/fromExampleFiles",
        name + (module != null ? "." + module : "") + ".parquet");
    outputFile.getParentFile().mkdirs();
    if(deleteIfExists) {
      outputFile.delete();
    }
    return outputFile;
  }

  public static File getCsvTestFile(String name, String module, boolean deleteIfExists) {
    File outputFile = new File("target/test/fromExampleFiles",
        name + (module != null ? "." + module : "") + ".csv");
    outputFile.getParentFile().mkdirs();
    if(deleteIfExists) {
      outputFile.delete();
    }
    return outputFile;
  }

  public static void verify(File expectedCsvFile, File outputCsvFile) throws IOException {
    BufferedReader expected = null;
    BufferedReader out = null;
    try {
    expected = new BufferedReader(new FileReader(expectedCsvFile));
    out = new BufferedReader(new FileReader(outputCsvFile));
    String lineIn;
    String lineOut = null;
    int lineNumber = 0;
    while ((lineIn = expected.readLine()) != null && (lineOut = out.readLine()) != null) {
      ++ lineNumber;
      lineOut = lineOut.substring(lineOut.indexOf("\t") + 1);
      assertEquals("line " + lineNumber, lineIn, lineOut);
    }
    assertNull("line " + lineNumber, lineIn);
    assertNull("line " + lineNumber, out.readLine());
    } finally {
      Utils.closeQuietly(expected);
      Utils.closeQuietly(out);
    }
  }

  public static void verify(File expectedCsvFile, File outputCsvFile, boolean orderMatters) throws IOException {
    if(!orderMatters) {
      // sort the files before diff'ing them
      expectedCsvFile = sortFile(expectedCsvFile);
      outputCsvFile = sortFile(outputCsvFile);
    }
    verify(expectedCsvFile, outputCsvFile);
  }

  private static File sortFile(File inFile) throws IOException {
    File sortedFile = new File(inFile.getAbsolutePath().concat(".sorted"));
    Comparator<String> comparator = new Comparator<String>() {
      public int compare(String r1, String r2){
        return r1.compareTo(r2);
      }
    };
    List<File> l = ExternalSort.sortInBatch(inFile, comparator) ;
    ExternalSort.mergeSortedFiles(l, sortedFile, comparator);
    return sortedFile;
  }

  @SuppressWarnings("unused")
  private static File sortFileOld(File inFile) throws IOException {
    File sortedFile = new File(inFile.getAbsolutePath().concat(".sorted"));
    BufferedReader reader = new BufferedReader(new FileReader(inFile));
    PrintWriter out = new PrintWriter(new FileWriter(sortedFile));

    try {
      String inputLine;
      List<String> lineList = new ArrayList<String>();
      while ((inputLine = reader.readLine()) != null) {
        lineList.add(inputLine);
      }
      Collections.sort(lineList);

      for (String outputLine : lineList) {
        out.println(outputLine);
      }
      out.flush();
    } finally {
      closeQuietly(reader);
      closeQuietly(out);
    }
    return sortedFile;
  }
}
