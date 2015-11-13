package com.transwarp.hbase.bulkload.combine;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

public class CombineFileLineRecordReader extends RecordReader<MultiFileInputWritableComparable, Text> {

	private long start; // offset of the chunk;
	private long end; // end of the chunk;
	private long pos; // current pos

	private LineReader in;
	private FileSystem fs;
	private Path file; // path of hdfs
	private Seekable filePosition;
	private MultiFileInputWritableComparable key;
	private Text value; // value should be string(hadoop Text)

	private CompressionCodecFactory compressionCodecs = null;
	private CompressionCodec codec;
	private Decompressor decompressor;

	private boolean isCompressedInput() {
	    return (codec != null);
	}
	
	private long getFilePosition() throws IOException {
		long retVal;
		if (isCompressedInput() && null != filePosition) {
			retVal = filePosition.getPos();
		} else {
			retVal = pos;
		}
		return retVal;
	}
	
	private int maxBytesToConsume(long pos) {
		return isCompressedInput()
				? Integer.MAX_VALUE
				: (int) Math.min(Integer.MAX_VALUE, end - pos);
	}

	public CombineFileLineRecordReader(CombineFileSplit split, TaskAttemptContext context, 
			Integer index) throws IOException {
		fs = FileSystem.get(context.getConfiguration());
		this.file = split.getPath(index);
		this.start = split.getOffset(index);
		this.end = start + split.getLength(index);
	    compressionCodecs = new CompressionCodecFactory(context.getConfiguration());
	    codec = compressionCodecs.getCodec(file);

		FSDataInputStream fileIn = fs.open(file); // open the file
		
		if (isCompressedInput()) {
			decompressor = CodecPool.getDecompressor(codec);
			if (codec instanceof SplittableCompressionCodec) {
				final SplitCompressionInputStream cIn =
					((SplittableCompressionCodec)codec).createInputStream(
							fileIn, decompressor, start, end,
							SplittableCompressionCodec.READ_MODE.BYBLOCK);
				in = new LineReader(cIn, context.getConfiguration());
				start = cIn.getAdjustedStart();
				end = cIn.getAdjustedEnd();
				filePosition = cIn; // take pos from compressed stream
			} else {
				in = new LineReader(codec.createInputStream(fileIn, decompressor),
						context.getConfiguration());
				filePosition = fileIn;
			}
		} else {
			fileIn.seek(start);
			in = new LineReader(fileIn, context.getConfiguration());
			filePosition = fileIn;
		}

		if (start != 0) {
			start += in.readLine(new Text(),0,maxBytesToConsume(start));
		}

		this.pos = start;
	}

	public void initialize(InputSplit split, TaskAttemptContext context) 
			throws IOException, InterruptedException {
	}


	public boolean nextKeyValue() throws IOException {
		if (key == null) {
			key = new MultiFileInputWritableComparable();
			key.setFileName(file.getName());
		}
		key.setOffset(pos);
		if (value == null) {
			value = new Text();
		}
		int newSize = 0;
		if (getFilePosition() <= end) {
			newSize = in.readLine(value);
			pos += newSize;
		}
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

	public float getProgress() throws IOException {
		if (start == end) {
			return 0.0f;
		} else {
			try {
				return Math.min(1.0f, (getFilePosition() - start)
						/ (float) (end - start));
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}
	}

	public MultiFileInputWritableComparable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	public synchronized void close() throws IOException {
		try {
			if (in != null) {
				in.close();
			}
		} finally {
			if (decompressor != null) {
				CodecPool.returnDecompressor(decompressor);
			}
		}
	}
}