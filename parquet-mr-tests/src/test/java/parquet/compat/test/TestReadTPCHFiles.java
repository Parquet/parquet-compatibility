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

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import parquet.Log;
import parquet.column.page.PageReadStore;
import parquet.example.data.Group;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.RecordReader;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;

public class TestReadTPCHFiles {
  private static final Log LOG = Log.getLog(TestReadTPCHFiles.class);

  @Test
    public void testTpch() throws IOException {
      File baseDir = new File("../testdata/tpch");
      final File[] csvFiles = baseDir.listFiles(new FilenameFilter() {
          public boolean accept(File dir, String name) {
          return name.endsWith(".csv");
          }
          });

      for (File csvFile : csvFiles) {
        testMrToImpala(csvFile, "|");
      }
    }

  @Test
    public void readTest() throws IOException {
      File baseDir = new File("../testdata/tpch");
      final File[] parquetFiles = baseDir.listFiles(new FilenameFilter() {
          public boolean accept(File dir, String name) {
          return name.endsWith(".parquet");
          }
          });

      for (File parquetFile : parquetFiles) {
        convertToCSV(parquetFile);
      }
    }

  private void testMrToImpala(File csvFile, String delimiter) throws IOException {
    LOG.info("Converting csv file to parquet using MR: " + csvFile);
    File parquetFile = convertToParquet(csvFile, delimiter);
    // TODO: try to read this file from Impala
  }

  static String readFile(String path) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(path));
    String line = null;
    StringBuilder stringBuilder = new StringBuilder();
    String ls = System.getProperty("line.separator");

    while ((line = reader.readLine()) != null ) {
      stringBuilder.append(line);
      stringBuilder.append(ls);
    }

    return stringBuilder.toString();
  }

  private File convertToParquet(File csvFile, String delimiter) throws IOException {
    File schemaFile = new File(csvFile.getParentFile(),
        csvFile.getName().substring(
          0, csvFile.getName().length() - ".csv".length()) + ".schema");
    String rawSchema = readFile(schemaFile.getAbsolutePath());

    File outputFile = new File("target/test/fromExampleFiles",
        csvFile.getName()+".writeFromJava.parquet");
    outputFile.delete();

    outputFile.getParentFile().mkdirs();
    Path path = new Path(outputFile.toURI());

    MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
    CsvParquetWriter writer = new CsvParquetWriter(path, schema);

    BufferedReader br = new BufferedReader(new FileReader(csvFile));
    String line;
    int lineNumber = 0;
    while ((line = br.readLine()) != null) {
      try {
        String[] fields = line.split(Pattern.quote(delimiter));
        writer.write(Arrays.asList(fields));
        ++ lineNumber;
      } catch (RuntimeException e) {
        throw new RuntimeException(
            format("error converting line %d to Parquet in %s", lineNumber, csvFile.getPath()),
            e);
      }
    }
    br.close();
    writer.close();
    return outputFile;
  }

  private void convertToCSV(File parquetFile) throws IOException {
    LOG.info("converting " + parquetFile.getName());
    Path testInputFile = new Path(parquetFile.toURI());
    File expectedOutputFile = new File(
        parquetFile.getParentFile(),
        parquetFile.getName().substring(0, parquetFile.getName().length() - ".parquet".length()) + ".csv");
    File csvOutputFile = new File("target/test/fromExampleFiles", parquetFile.getName()+".readFromJava.csv");
    csvOutputFile.getParentFile().mkdirs();
    Configuration configuration = new Configuration(true);
    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, testInputFile);
    MessageType schema = readFooter.getFileMetaData().getSchema();
    ParquetFileReader parquetFileReader = new ParquetFileReader(configuration, testInputFile, readFooter.getBlocks(), schema.getColumns());
    PageReadStore pages = parquetFileReader.readNextRowGroup();
    final long rows = pages.getRowCount();
    LOG.info("rows: "+rows);
    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
    BufferedWriter w = new BufferedWriter(new FileWriter(csvOutputFile));
    try {
      for (int i = 0; i < rows; i++) {
        final Group g = recordReader.read();
        for (int j = 0; j < schema.getFieldCount(); j++) {
          final Type type = schema.getFields().get(j);
          if (j > 0) {
            w.write('|');
          }
          String valueToString = g.getValueToString(j, 0);// no repetition here
          if (type.isPrimitive()
              && (type.asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.FLOAT
                || type.asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.DOUBLE)
              && valueToString.endsWith(".0")) {
            valueToString = valueToString.substring(0, valueToString.length() - 2);
          }
          w.write(valueToString);
        }
        w.write('\n');
      }
    } finally {
      w.close();
    }
    verify(expectedOutputFile, csvOutputFile);
    LOG.info("verified " + parquetFile.getName());
  }

  private void verify(File expectedCsvFile, File outputCsvFile) throws IOException {
    final BufferedReader expected = new BufferedReader(new FileReader(expectedCsvFile));
    final BufferedReader out = new BufferedReader(new FileReader(outputCsvFile));
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
    expected.close();
    out.close();
  }
}
