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

public class Utils {

  private static final Log LOG = Log.getLog(Utils.class);

  public static File[] getAllOriginalCSVFiles() {
    File baseDir = new File("../testdata/tpch");
    final File[] csvFiles = baseDir.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.endsWith(".csv");
      }
    });
    return csvFiles;
  }

  public static final String CSV_DELIMITER= "|";

  private static String readFile(String path) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(path));
    String line = null;
    StringBuilder stringBuilder = new StringBuilder();
    String ls = System.getProperty("line.separator");

    while ((line = reader.readLine()) != null ) {
      stringBuilder.append(line);
      stringBuilder.append(ls);
    }

    reader.close();

    return stringBuilder.toString();
  }

  public static String getSchema(File csvFile) throws IOException {
    String fileName = csvFile.getName().substring(
        0, csvFile.getName().length() - ".csv".length()) + ".schema";
    File schemaFile = new File(csvFile.getParentFile(), fileName);
    return readFile(schemaFile.getAbsolutePath());
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
      br.close();
      writer.close();
    } 
  }

  public static void convertParquetToCSV(File parquetFile, File csvOutputFile) throws IOException {
    // TODO argument checks - parquetfile must end in .parquet
    // csv file must end in .csv

    LOG.info("Converting " + parquetFile.getName() + " to " + csvOutputFile.getName());

    if(csvOutputFile.exists()) {
      throw new IOException("Output file " + csvOutputFile.getAbsolutePath() + 
          " already exists");
    }
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
      } 
    } finally {
      parquetFileReader.close();
      w.close();
    }
  }

  public static void verify(File expectedCsvFile, File outputCsvFile) throws IOException {
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

  public static String getFileNamePrefix(File file) {
    return file.getName().substring(0, file.getName().indexOf("."));
  }

  public static File getParquetTestFile(String prefix, boolean deleteIfExists) {
    File outputFile = new File("target/test/fromExampleFiles",
        prefix+".parquet");
    outputFile.getParentFile().mkdirs();
    if(deleteIfExists) {
      outputFile.delete();
    }
    return outputFile;
  }

  public static File getCsvTestFile(String prefix , boolean deleteIfExists) {
    File outputFile = new File("target/test/fromExampleFiles",
        prefix+".csv");
    outputFile.getParentFile().mkdirs();
    if(deleteIfExists) {
      outputFile.delete();
    }
    return outputFile;
  }
}
