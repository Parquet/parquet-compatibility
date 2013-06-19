package parquet.compat.test;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import parquet.Log;

/**
 * This tests compatibility of parquet format (written by java code)
 * from older versions of parquet with the current version.
 * 
 * TODO git is not an ideal place to track binary files, 
 * we should find a better place to store these.
 * (eg- versioned jar files under maven central etc.)
 * 
 * Parquet files for each of the version are assumed to be checked in
 * under $PROJECT_HOME/testdata/tpch/$version
 * 
 * @author amokashi
 *
 */
public class TestBackwardsCompatibility {

  private static final Log LOG = Log.getLog(TestBackwardsCompatibility.class);

  public static final String[] COMPATIBLE_VERSIONS = {
    "1.0.0-SNAPSHOT"
  };

  @Test
  public void testParquetBackwardsCompatibility() throws IOException {
    // read all versions of parquet files and convert them into csv
    // diff the csvs with original csvs
    File[] originalCsvFiles = Utils.getAllOriginalCSVFiles();

    for (String version : COMPATIBLE_VERSIONS) {
      LOG.info("Testing compatibility with " + COMPATIBLE_VERSIONS);
      for(File originalCsvFile : originalCsvFiles) {
        File versionParquetFile = Utils.getParquetVersionedFile(
              Utils.getFileNamePrefix(originalCsvFile), version, true);
        
        File csvVersionedTestFile = Utils.getCsvTestFile(
            versionParquetFile.getName()+"."+version, true);

        Utils.convertParquetToCSV(versionParquetFile, csvVersionedTestFile);

        Utils.verify(originalCsvFile, csvVersionedTestFile);
      }
    }
  }

  @Test
  public void testReadWriteCompatibility() throws IOException {
    // convert original csv into parquet
    // convert parquet back into csv
    // diff converted csv with original csv

    File[] csvFiles = Utils.getAllOriginalCSVFiles();
    for (File csvFile : csvFiles) {
      File parquetTestFile = Utils.getParquetTestFile(
          Utils.getFileNamePrefix(csvFile)+".java.readwrite", true);
      Utils.convertCsvToParquet(csvFile, parquetTestFile);

      File csvTestFile = Utils.getCsvTestFile(
          Utils.getFileNamePrefix(csvFile)+".java.readwrite", true);
      Utils.convertParquetToCSV(parquetTestFile, csvTestFile);

      Utils.verify(csvTestFile, csvFile);
    }
  }
}
