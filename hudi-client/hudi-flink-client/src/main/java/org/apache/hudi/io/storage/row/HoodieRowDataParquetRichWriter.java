package org.apache.hudi.io.storage.row;

import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.io.storage.HoodieParquetConfig;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * @author zhouyu
 * @date 2023/12/11
 */
public class HoodieRowDataParquetRichWriter implements HoodieRowDataFileWriter {

    private static final Logger logger = LoggerFactory.getLogger(HoodieRowDataParquetRichWriter.class);

    private final Path file;
    private final HoodieWrapperFileSystem fs;
    private final long maxFileSize;
    private final HoodieRowDataParquetWriteSupport writeSupport;

    private final ParquetWriter<RowData> parquetWriter;

    public HoodieRowDataParquetRichWriter(Path file, HoodieParquetConfig<HoodieRowDataParquetWriteSupport> parquetConfig) throws IOException {

        logger.info("{} will use  HoodieRowDataParquetRichWriter config:{}", file, parquetConfig);

        this.file = HoodieWrapperFileSystem.convertToHoodiePath(file, parquetConfig.getHadoopConf());
        this.fs = (HoodieWrapperFileSystem) this.file.getFileSystem(FSUtils.registerFileSystem(file,
                parquetConfig.getHadoopConf()));
        this.maxFileSize = parquetConfig.getMaxFileSize()
                + Math.round(parquetConfig.getMaxFileSize() * parquetConfig.getCompressionRatio());
        this.writeSupport = parquetConfig.getWriteSupport();

        Builder parquetWriterBuilder = new Builder(HoodieWrapperFileSystem.convertToHoodiePath(file, parquetConfig.getHadoopConf()), parquetConfig)
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withCompressionCodec(parquetConfig.getCompressionCodecName())
                .withRowGroupSize((long)parquetConfig.getBlockSize())
                .withValidation(ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED)
                .withConf(FSUtils.registerFileSystem(file, parquetConfig.getHadoopConf()))
                .withMaxPaddingSize(ParquetWriter.MAX_PADDING_SIZE_DEFAULT)
                .withPageSize(parquetConfig.getPageSize())
                .withDictionaryPageSize(parquetConfig.getPageSize())
                .withDictionaryEncoding(parquetConfig.dictionaryEnabled())
                .withWriterVersion(ParquetWriter.DEFAULT_WRITER_VERSION)
                .withEncryption(null);

        try {
            Field f = Builder.class.getSuperclass().getDeclaredField("encodingPropsBuilder");
            f.setAccessible(true);
            ParquetProperties.Builder pb = (ParquetProperties.Builder)f.get(parquetWriterBuilder);
            pb.withMaxRowCountForPageSizeCheck(parquetConfig.getMaxRowCountForPageSizeCheck());
            pb.withMinRowCountForPageSizeCheck(parquetConfig.getMinRowCountForPageSizeCheck());
            f.setAccessible(false);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            logger.warn(e.toString());
        }

        parquetWriter = parquetWriterBuilder.build();

    }

    @Override
    public boolean canWrite() {
        return fs.getBytesWritten(file) < maxFileSize;
    }

    @Override
    public void writeRow(String key, RowData row) throws IOException {
        parquetWriter.write(row);
        writeSupport.add(key);
    }

    @Override
    public void writeRow(RowData row) throws IOException {
        parquetWriter.write(row);
    }

    @Override
    public void close() throws IOException {
        parquetWriter.close();
    }


    public static class Builder extends ParquetWriter.Builder<RowData, Builder> {

        HoodieParquetConfig<HoodieRowDataParquetWriteSupport>parquetConfig;

        protected Builder(Path file, HoodieParquetConfig<HoodieRowDataParquetWriteSupport> parquetConfig) {
            super(file);
            this.parquetConfig = parquetConfig;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected WriteSupport<RowData> getWriteSupport(Configuration conf) {
            return parquetConfig.getWriteSupport();
        }
    }

}
