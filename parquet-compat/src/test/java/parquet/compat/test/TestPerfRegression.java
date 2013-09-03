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

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import parquet.Log;

public class TestPerfRegression {
  private static final Log LOG = Log.getLog(TestPerfRegression.class);
  
  private static final int fileSizeMegs = 1000;
  
  @Test
  public void testWritePerf() throws IOException {
    // With no dictionary - default
    File csvTestFile = Utils.createTestFile(fileSizeMegs);
    
    File parquetTestFile = Utils.getParquetOutputFile("perf", String.valueOf(fileSizeMegs), true);
    long startTime = System.currentTimeMillis();
    ConvertUtils.convertCsvToParquet(csvTestFile, parquetTestFile);
    long endTime = System.currentTimeMillis();
    
    long totalTime = (endTime - startTime);
    LOG.info("Write Time: " + totalTime ); 
    
    Utils.writePerfResult("write", totalTime);
    
    LOG.info("Time taken to write " + fileSizeMegs + " sized csv file : " + fileSizeMegs);
    
    assertTrue(totalTime < 60000);
    
    // It should not be slower than previous versions
    for(String version : Utils.getAllPreviousVersionDirs()) {
      long totalTimeForVersion = Utils.readPerfResult(version, "write");
      LOG.info("Time taken to write with version "+ version + ": " + totalTimeForVersion);
      assertTrue(totalTime < 1.1 * totalTimeForVersion);
    }
  }
  
  @Test
  public void testReadPerf() throws IOException {
    File parquetTestFile = Utils.getParquetOutputFile("perf", String.valueOf(fileSizeMegs), false);
    if(!parquetTestFile.exists()) {
      throw new IOException("File "+ parquetTestFile.getName() + " does not exists, Run testWritePerf");
    }
    File csvTestFile = Utils.getCsvTestFile("perf", String.valueOf(fileSizeMegs), true);
    long startTime = System.currentTimeMillis();
    ConvertUtils.convertParquetToCSV(parquetTestFile, csvTestFile );
    long endTime = System.currentTimeMillis();
    
    long totalTime = (endTime - startTime);
    LOG.info("Read Time: " + totalTime ); 
    
    Utils.writePerfResult("read", totalTime);
    
    assertTrue(totalTime < 45000);
    
    // It should not be slower than previous versions
    for(String version : Utils.getAllPreviousVersionDirs()) {
      long totalTimeForVersion = Utils.readPerfResult(version, "read");
      LOG.info("Time taken to read with version "+ version + ": " + totalTimeForVersion);
      assertTrue(totalTime < 1.1 * totalTimeForVersion);
    }
  }

}
