package com.transwarp.hbase.bulkload.common;

import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;

public class BulkLoadUtils {
	public static Algorithm getCompressionTypeByString(String algorithmType) {
		try {
			return Algorithm.valueOf(algorithmType.toUpperCase());
		} catch (Exception e) {
			return Algorithm.NONE;
		}
	}
	
	public static BloomType getBloomTypeByString(String bloomTypeStr) {
		try {
			return BloomType.valueOf(bloomTypeStr.toUpperCase());
		} catch (Exception e) {
			return BloomType.NONE;
		}
	}
	
	public static DataBlockEncoding getDataBlockEncodingByString(String dataBlockEncodingStr) {
		try {
			return DataBlockEncoding.valueOf(dataBlockEncodingStr.toUpperCase());
		} catch (Exception e) {
			return DataBlockEncoding.NONE;
		}
	}

}
