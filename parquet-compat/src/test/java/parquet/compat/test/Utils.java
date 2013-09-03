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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.file.tfile.Utils.Version;

import parquet.Log;

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

  public static void writePerfResult(String module, long millis) throws IOException{
    PrintWriter writer = null;
    try {
      File outputFile = new File("target/test/perftime." + module + ".txt");
      outputFile.delete();
      writer = new  PrintWriter(outputFile);
      writer.write(String.valueOf(millis));
    } finally {
      closeQuietly(writer);
    }
  }
  
  public static long readPerfResult(String version, String module) throws IOException {
    BufferedReader reader = null;
    try {
      File inFile = new File("../" + version + "/target/test/perftime." + module + ".txt");
      reader = new  BufferedReader(new FileReader(inFile));
      return Long.parseLong(reader.readLine());
    } finally {
      closeQuietly(reader);
    }
  }
  
  public static File createTestFile(long largerThanMB) throws IOException {
    File outputFile = new File("target/test/csv/perftest.csv");
    if(outputFile.exists()) {
      return outputFile;
    }
    File toCopy = new File("../parquet-testdata/tpch/customer.csv");
    FileUtils.copyFile(new File("../parquet-testdata/tpch/customer.schema"), new File("target/test/csv/perftest.schema"));
    
    OutputStream output = null;
    InputStream input = null;
    
    try {
      output = new BufferedOutputStream(new FileOutputStream(outputFile, true));
      input = new BufferedInputStream(new FileInputStream(toCopy));
      input.mark(Integer.MAX_VALUE);
      while(outputFile.length() <= largerThanMB * 1024 * 1024) {
        //appendFile(output, toCopy);
        IOUtils.copy(input, output);
        input.reset();
      }
    } finally {
      closeQuietly(input);
      closeQuietly(output);
    }
    
    return outputFile;
  }
  
  public static File[] getAllOriginalCSVFiles() {
    File baseDir = new File("../parquet-testdata/tpch");
    final File[] csvFiles = baseDir.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.endsWith(".csv");
      }
    });
    return csvFiles;
  }

  public static String[] getAllPreviousVersionDirs() throws IOException {
    File baseDir = new File("..");
    final String currentVersion = getCurrentVersion();
    final String[] versions = baseDir.list(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.startsWith("parquet-compat-") 
            && new Version(name.replace("parquet-compat-", "")).compareTo(new Version(currentVersion)) < 0;
      }
    });
    return versions;
  }
  
  static class Version implements Comparable<Version> {
    int major;
    int minor;
    int minorminor;
    String tag;
    
    Version(String versionStr) {
      String[] versions = versionStr.split("\\.");
      int size = versions.length;
      if (size > 0) {
        this.major = Integer.parseInt(versions[0]);
      }
      if (size > 1) {
        this.minor = Integer.parseInt(versions[1]);
      }
      if (size > 2) {
        if(versions[2].contains("-")) {
          String[] minorMin = versions[2].split("-");
          this.minorminor = Integer.parseInt(minorMin[0]);
          this.tag = minorMin[1];
        } else {
          this.minorminor = Integer.parseInt(versions[2]);
        }
      }
      if(size == 4) {
        this.tag = versions[3];
      }
      if (size > 4) {
        throw new RuntimeException("Illegal version number " + versionStr);
      }
    }
    
    public int compareMajorMinor(Version o) {
      return ComparisonChain.
          start().
      compare(major, o.major).
      compare(minor, o.minor).
      result();
    }

    @Override
    public int compareTo(Version o) {
      return ComparisonChain.
          start().
      compare(major, o.major).
      compare(minor, o.minor).
      compare(minorminor, o.minorminor).
      compare(tag, o.tag).
      result();
    }
    
    // Very basic implementation of comparisonchain
    private static class ComparisonChain {
      int result = 0;
      private ComparisonChain(int result) {
        this.result = result; 
      }
      static ComparisonChain start() {
        return new ComparisonChain(0);
      }
      ComparisonChain compare(String a, String b) {
        if (result != 0) {
          return this;
        }
        if(b == null) {
          if (a!= null) result=1;
          else result= 0;
        } else if (a == null) {
          result=1;
        } else if (result == 0) {
          result = a.compareTo(b);
        }
        return this;
      }
      ComparisonChain compare(int a, int b) {
        if (result == 0) {
          result = Integer.compare(a, b);
        }
        return this;
      }
      int result() {
        return result;
      }
    }    
  }
  
  
  public static File getParquetOutputFile(String name, String module, boolean deleteIfExists) {
    File outputFile = new File("target/parquet/", getParquetFileName(name, module));
    outputFile.getParentFile().mkdirs();
    if(deleteIfExists) {
      outputFile.delete();
    }
    return outputFile;
  }

  private static String getParquetFileName (String name, String module) {
    return name + (module != null ? "." + module : "") + ".parquet";
  }

  public static File getParquetFile(String name, String version, String module, boolean failIfNotExist) 
      throws IOException {
    File parquetFile = new File("../"+version+"/target/parquet/", getParquetFileName(name, module));
    parquetFile.getParentFile().mkdirs();
    if(!parquetFile.exists()) {
      String msg = "File " + parquetFile.getAbsolutePath() + " does not exist";
      if(failIfNotExist) {
        throw new IOException(msg);
      }
      LOG.warn(msg);
    }
    return parquetFile;
  }
  
  private static String getCurrentVersion() throws IOException {
    return new File(".").getCanonicalFile().getName().replace("parquet-compat-", "");
  }
  
  public static String[] getImpalaDirectories() throws IOException {
    File baseDir = new File("../parquet-testdata/impala");
    final String currentVersion = getCurrentVersion();
    final String[] impalaVersions = baseDir.list(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        if (name.startsWith(".")) {
          return false;
        }
        if (name.contains("-")) {
          name = name.split("-")[0];
        }
        return new Version(name).compareMajorMinor(new Version(currentVersion)) == 0;
      }
    });
    return impalaVersions;
  }

  public static File getParquetImpalaFile(String name, String impalaVersion) throws IOException {
    String fileName = name + ".impala.parquet";
    File parquetFile = new File("../parquet-testdata/impala/" + impalaVersion, fileName);
    if(!parquetFile.exists()) {
      throw new IOException("File " + fileName + " does not exist");
    }
    return parquetFile;
  }

  public static String getFileNamePrefix(File file) {
    return file.getName().substring(0, file.getName().indexOf("."));
  }

  public static File getCsvTestFile(String name, String module, boolean deleteIfExists) {
    File outputFile = new File("target/test/csv/",
        name + (module != null ? "." + module : "") + ".csv");
    outputFile.getParentFile().mkdirs();
    if(deleteIfExists) {
      outputFile.delete();
    }
    return outputFile;
  }

  public static File getParquetTestFile(String name, String module, boolean deleteIfExists) {
    File outputFile = new File("target/test/parquet/",
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
