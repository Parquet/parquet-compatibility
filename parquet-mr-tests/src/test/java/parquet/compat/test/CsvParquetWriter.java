package parquet.compat.test;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;

import parquet.hadoop.ParquetWriter;
import parquet.hadoop.api.WriteSupport;
import parquet.schema.MessageType;

public class CsvParquetWriter extends ParquetWriter<List<String>> {
  public CsvParquetWriter(Path file, MessageType schema) throws IOException {
	super(file, (WriteSupport<List<String>>) new CsvWriteSupport(schema));
  }
}
