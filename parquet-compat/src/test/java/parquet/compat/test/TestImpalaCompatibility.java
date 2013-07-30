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

public class TestImpalaCompatibility {
  private static final Log LOG = Log.getLog(TestImpalaCompatibility.class);

  @Test
  public void testReadFromImpala() throws IOException {
    
    File[] originalCsvFiles = Utils.getAllOriginalCSVFiles();
    String[] impalaVersions = Utils.getImpalaDirectories();
    LOG.info("Testing compatibility in reading files written by impala");

    for(String impalaVersion : impalaVersions) {
      for(File originalCsv : originalCsvFiles) {
        String prefix = Utils.getFileNamePrefix(originalCsv);
        File parquetFile = null;
        try {
        parquetFile = Utils.getParquetImpalaFile(prefix, impalaVersion);
        } catch (Exception e){continue;}
        File csvOutputFile = Utils.getCsvTestFile(prefix, "impala", true);
        ConvertUtils.convertParquetToCSV(parquetFile, csvOutputFile);
        Utils.verify(originalCsv, csvOutputFile, false);
      }
    }
  }
}
