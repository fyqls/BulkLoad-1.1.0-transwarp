package com.transwarp.hbase.bulkload.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

/*
 * The class is used to generate a fine tuned region splits based on an adequate sampling
 * 
 * You input a file containing record distribution information based on a sample, for example data of one 
 * out of a year
 */

public class RegionSplitKeyUtils {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	public static void main(String[] args) throws NumberFormatException, IOException {

		if (args.length != 2) {
			System.out.println("Usage RegionSplitKeyGen <recordCountFile> <recordsPerRegion>");
			System.exit(1);
		}

		// wap_dist.txt 500000

		String file = args[0];
		long sampleRecordsPerRegion = Long.parseLong(args[1]);

		System.out.println("splitKeySpec="+GenSplitKeys(file, sampleRecordsPerRegion));
		
	}
	
	private static String GenSplitKeys(String name, long sampleRecordsPerRegion) throws NumberFormatException, IOException {
	  File dir = new File(name);
	  if (dir.isDirectory()) {
	    return genSplitKeysFromDir(name, sampleRecordsPerRegion);
	  } else {
	    return genSplitKeysFromFile(name, sampleRecordsPerRegion);
	  }
	}
	
	static class Pair<T1, T2> {
	  T1 t1;
	  T2 t2;
	  Pair(T1 t1, T2 t2) {
	    this.t1 = t1;
	    this.t2 = t2;
	  }
	  
	  public T1 getFirst() {
	    return t1;
	  }
	  
	  public T2 getSecond() {
	    return t2;
	  }
	  
	  public void setFirst(T1 t1) {
	    this.t1 = t1;
	  }
	  
	  public void setSecond(T2 t2) {
	    this.t2 = t2;
	  }
	}
	
	static class PairComparator implements Comparator<Pair<BufferedReader, Pair<String, Long>>> {
	  public int compare(Pair<BufferedReader, Pair<String, Long>> p1, Pair<BufferedReader, Pair<String, Long>> p2) {
	    return p1.getSecond().getFirst().compareTo(p2.getSecond().getFirst());
	  }
	}
	
	private static String genSplitKeysFromDir(String name,
      long sampleRecordsPerRegion) throws NumberFormatException, IOException {
	  File dir = new File(name);
	  String[] files = dir.list();
	  PriorityQueue<Pair<BufferedReader, Pair<String, Long>>> queue = new PriorityQueue<Pair<BufferedReader, Pair<String, Long>>>(1000, new PairComparator());
	  for (String s : files) {
	    if (s.startsWith(".")) {
	      continue;
	    }
	    String path = name + "/" + s;
	    BufferedReader reader = new BufferedReader(new FileReader(path));
	    Pair<BufferedReader, Pair<String, Long>> obj = new Pair<BufferedReader, Pair<String, Long>>(reader, new Pair<String, Long>(null, null));
	    obj = update(obj);
	    if (obj != null) {
	    //  Pair<BufferedReader, Pair<String, Long>> uobj = update(obj);
	    //  if (uobj != null)
	     //   queue.add(uobj);
	    	queue.add(obj);
	   }
	    	
	  }
	  
	  Pair<BufferedReader, Pair<String, Long>> p = null;
	  long total = 0;
	  StringBuffer sb = new StringBuffer();
	  long max = Long.MIN_VALUE;
    long min = Long.MAX_VALUE;
    
	  while ((p = queue.poll()) != null) {
	    total += p.getSecond().getSecond();
	    if (total >= sampleRecordsPerRegion) {
	      System.out.println("find split key:" + p.getSecond().getFirst());
	      sb.append(p.getSecond().getFirst()).append(",");

        if (total > max)
          max = total;
        if (total < min)
          min = total;
        total = 0;
	    }
	    if ((p = update(p)) != null) {
	      queue.add(p);
	    }
	  }
	  String result = sb.toString();
    if (result.endsWith(","))
      result = result.substring(0, result.length() - 1);

    return result;
	}
	
	private static Pair<BufferedReader, Pair<String, Long>> update(Pair<BufferedReader, Pair<String, Long>> reader) throws NumberFormatException, IOException {
	  String line = null;
	  while((line = reader.getFirst().readLine()) != null) {
      String[] ss = line.split(",");
      if (ss.length != 2) {
        System.out.println("Bad line found: " + line);
        continue;
      }
      reader.getSecond().setFirst(ss[0]);
      long n = Long.parseLong(ss[1].trim());
      reader.getSecond().setSecond(n);
      return reader;
	  }
	  return null;
	}

	/*
	 * @param path the file path generate
	 */
	private static String genSplitKeysFromFile(String sampleInfoFile,
			long sampleRecordsPerRegion) {

		StringBuffer sb = new StringBuffer();
		try {
			System.out.println("====sampleInfoFile:"+sampleInfoFile);
			BufferedReader reader = new BufferedReader(new FileReader(
					sampleInfoFile));
			System.out.println("====reader:"+reader);
			String line = reader.readLine();
			System.out.println("====firstLine:"+line);
			String[] ss = line.split(",");
			if (ss.length != 2) {
				System.out.println("Bad line found: " + line);
			}
			long n = Long.parseLong(ss[ss.length - 1].trim());
			long records = n;

			long max = Long.MIN_VALUE;
			long min = Long.MAX_VALUE;

			while ((line = reader.readLine()) != null) {
				ss = line.split(",");
				if (ss.length != 2) {
					System.out.println("Bad line found: " + line);
				}
				n = Long.parseLong(ss[ss.length - 1].trim());
				if (records + n > sampleRecordsPerRegion) {
					sb.append(ss[0]).append(",");

					if (records > max)
						max = records;
					if (records < min)
						min = records;
					records = 0;
				} else {
					records += n;
				}
			}

			System.out.println("# Max rows per region: " + max);
			System.out.println("# Min rows per region: " + min);
			
			reader.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

		String result = sb.toString();
		if (result.endsWith(","))
			result = result.substring(0, result.length() - 1);

		return result;
	}

}
