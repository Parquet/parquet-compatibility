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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.Log;
import parquet.Preconditions;
import parquet.column.page.PageReadStore;
import parquet.example.data.Group;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.RecordReader;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.Type;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

public class ConvertUtils {

  private static final Log LOG = Log.getLog(ConvertUtils.class);

  public static final String CSV_DELIMITER= "|";

  private static String readFile(String path) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(path));
    StringBuilder stringBuilder = new StringBuilder();

    try {
      String line = null;
      String ls = System.getProperty("line.separator");

      while ((line = reader.readLine()) != null ) {
        stringBuilder.append(line);
        stringBuilder.append(ls);
      }
    } finally {
      Utils.closeQuietly(reader);
    }

    return stringBuilder.toString();
  }

  public static String getSchema(File csvFile) throws IOException {
    String fileName = csvFile.getName().substring(
        0, csvFile.getName().length() - ".csv".length()) + ".schema";
    File schemaFile = new File(csvFile.getParentFile(), fileName);
    return readFile(schemaFile.getAbsolutePath());
  }

  public static void convertCsvToParquet(File csvFile, File outputParquetFile) throws IOException {
    LOG.info("Converting " + csvFile.getName() + " to " + outputParquetFile.getName());
    String rawSchema = getSchema(csvFile);
    if(outputParquetFile.exists()) {
      throw new IOException("Output file " + outputParquetFile.getAbsolutePath() + 
          " already exists");
    }

    Path path = new Path(outputParquetFile.toURI());

    MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
    CsvParquetWriter writer = new CsvParquetWriter(path, schema);

    BufferedReader br = new BufferedReader(new FileReader(csvFile));
    String line;
    int lineNumber = 0;
    try {
      while ((line = br.readLine()) != null) {
        String[] fields = line.split(Pattern.quote(CSV_DELIMITER));
        writer.write(Arrays.asList(fields));
        ++lineNumber;
      }
    }catch (RuntimeException e) {
      throw new RuntimeException(
          format("error converting line %d to Parquet in %s", lineNumber, csvFile.getPath()),
          e);
    } finally {
      LOG.info("Number of lines: " + lineNumber);
      Utils.closeQuietly(br);
      Utils.closeQuietly(writer);
    } 
  }

  public static void convertParquetToCSV(File parquetFile, File csvOutputFile) throws IOException {
    Preconditions.checkArgument(parquetFile.getName().endsWith(".parquet"), 
        "parquet file should have .parquet extension");
    Preconditions.checkArgument(csvOutputFile.getName().endsWith(".csv"), 
        "csv file should have .csv extension");
    Preconditions.checkArgument(!csvOutputFile.exists(), 
        "Output file " + csvOutputFile.getAbsolutePath() + " already exists");

    LOG.info("Converting " + parquetFile.getName() + " to " + csvOutputFile.getName());


    Path parquetFilePath = new Path(parquetFile.toURI());

    Configuration configuration = new Configuration(true);

    GroupReadSupport readSupport = new GroupReadSupport();
    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
    MessageType schema = readFooter.getFileMetaData().getSchema();

    readSupport.init(configuration, null, schema);
    BufferedWriter w = new BufferedWriter(new FileWriter(csvOutputFile));
    ParquetReader<Group> reader = new ParquetReader<Group>(parquetFilePath, readSupport);
    try{
      Group g = null;
      while( (g = reader.read())!= null) {
        writeGroup(w, g, schema);
      }
    }
    finally {
      Utils.closeQuietly(reader);
      Utils.closeQuietly(w);
    }
  }

  private static void writeGroup(BufferedWriter w, Group g, MessageType schema) 
      throws IOException{
    for (int j = 0; j < schema.getFieldCount(); j++) {
      final Type type = schema.getFields().get(j);
      if (j > 0) {
        w.write(CSV_DELIMITER);
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

  @Deprecated
  public static void convertParquetToCSVEx(File parquetFile, File csvOutputFile) throws IOException {
    Preconditions.checkArgument(parquetFile.getName().endsWith(".parquet"), 
        "parquet file should have .parquet extension");
    Preconditions.checkArgument(csvOutputFile.getName().endsWith(".csv"), 
        "csv file should have .csv extension");
    Preconditions.checkArgument(!csvOutputFile.exists(), 
        "Output file " + csvOutputFile.getAbsolutePath() + " already exists");

    LOG.info("Converting " + parquetFile.getName() + " to " + csvOutputFile.getName());

    Path parquetFilePath = new Path(parquetFile.toURI());

    Configuration configuration = new Configuration(true);

    // TODO Following can be changed by using ParquetReader instead of ParquetFileReader
    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
    MessageType schema = readFooter.getFileMetaData().getSchema();
    ParquetFileReader parquetFileReader = new ParquetFileReader(
        configuration, parquetFilePath, readFooter.getBlocks(), schema.getColumns());
    BufferedWriter w = new BufferedWriter(new FileWriter(csvOutputFile));
    PageReadStore pages = null;
    try {
      while (null != (pages = parquetFileReader.readNextRowGroup())) {
        final long rows = pages.getRowCount();
        LOG.info("Number of rows: " + rows);

        final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
        for (int i = 0; i < rows; i++) {
          final Group g = recordReader.read();
          writeGroup(w, g, schema);
        }
      } 
    } finally {
      Utils.closeQuietly(parquetFileReader);
      Utils.closeQuietly(w);
    }
  }

}
