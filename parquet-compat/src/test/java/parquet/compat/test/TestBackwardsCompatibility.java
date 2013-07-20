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

import org.junit.Test;

import parquet.Log;

/**
 * This tests compatibility of parquet format (written by java code)
 * from older versions of parquet with the current version.
 * 
 * Parquet files for previous versions are assumed to be generated under
 * under $PROJECT_HOME/parquet-compat-$version/target/parquet/
 * If files are not present, a WARNing is generated.
 * 
 * @author amokashi
 *
 */
public class TestBackwardsCompatibility {

  private static final Log LOG = Log.getLog(TestBackwardsCompatibility.class);
  
  @Test
  public void testReadWriteCompatibility() throws IOException {
    File[] csvFiles = Utils.getAllOriginalCSVFiles();
    for (File csvFile : csvFiles) {
      String filename = Utils.getFileNamePrefix(csvFile);
      
      // With no dictionary - default
      File parquetTestFile = Utils.getParquetOutputFile(filename, "plain", true);
      ConvertUtils.convertCsvToParquet(csvFile, parquetTestFile);
      File csvTestFile = Utils.getCsvTestFile(filename, "plain", true);
      ConvertUtils.convertParquetToCSV(parquetTestFile, csvTestFile);

      Utils.verify(csvFile, csvTestFile);
      
      // With dictionary encoding
      parquetTestFile = Utils.getParquetOutputFile(filename, "dict", true);
      ConvertUtils.convertCsvToParquet(csvFile, parquetTestFile, true);
      csvTestFile = Utils.getCsvTestFile(filename, "dict", true);
      ConvertUtils.convertParquetToCSV(parquetTestFile, csvTestFile);

      Utils.verify(csvFile, csvTestFile);
    }
  }

  @Test
  public void testParquetBackwardsCompatibility() throws IOException {
    // read all versions of parquet files and convert them into csv
    // diff the csvs with original csvs
    File[] originalCsvFiles = Utils.getAllOriginalCSVFiles();
    String[] compatibleVersions = Utils.getAllPreviousVersionDirs();
    
    for (String version : compatibleVersions) {
      LOG.info("Testing compatibility with " + version);
      for(File originalCsvFile : originalCsvFiles) {
        
        String prefix = Utils.getFileNamePrefix(originalCsvFile);
        File versionParquetFile = Utils.getParquetFile(prefix, version, "plain", true);
        File csvVersionedTestFile = Utils.getCsvTestFile(prefix, version, true);

        ConvertUtils.convertParquetToCSV(versionParquetFile, csvVersionedTestFile);

        Utils.verify(originalCsvFile, csvVersionedTestFile);
      }
    }
  }
}
