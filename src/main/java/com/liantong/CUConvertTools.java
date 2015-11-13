package com.liantong;


/**
 * 转换函数库
 * @author yinweiqiang
 * @since 2012.10.15
 * @version 1.0
 * 
 * */
public class CUConvertTools {
	/**
	 * 时间转换
	 * 原时间字符串：2012-10-14 12:13:17
	 * 转换后时间字符串：20121014121317
	 * */
	public static String convertDatetime(String datetime) {
		String str = datetime.replace(" ", "");
		str = str.replace("-", "");
		str = str.replace(".", "");
		str = str.replace(":", "");
		return str;		
	}
	
	/**
	 * 转锟斤拷IP锟斤拷址
	 * 原锟斤拷式锟斤拷172.21.219.149
	 * 锟街革拷式锟斤拷172021219149
	 * 说锟斤拷锟斤拷前锟芥补0锟斤拷锟斤拷锟斤拷3位
	 * @throws Exception 
	 * */
	public static String convertIP(String ip){
		String[] cols = ip.split("\\.");
		if((cols != null) && (cols.length == 4)) {
			String str = "";
			for(int i = 0; i < 4; i ++) {
				String ipItem = "000" + cols[i];
				int len = ipItem.length();
				str = str + ipItem.substring(len - 3);
			}
			return str;
		}
		else {
			//System.out.println(CUErrorMessage.MESSAGE_IP_FORMAT_ERROR);
			return "000000000000";
		}
	}
	
	/**
	 * 转锟斤拷锟剿匡拷
	 * 原锟斤拷式锟斤拷80
	 * 锟街革拷式锟斤拷00080
	 * 
	 * */
	public static String convertPort(String port) {
		String str = "00000" + port;
		int len = str.length();
		return str.substring(len - 5);
	}
	
	/**
	 * 转锟斤拷URL
	 * 原锟斤拷式锟斤拷http://www.edenredticket.com/cn/EDM/360buy-card.htm
	 * 锟街革拷式锟斤拷com.edenredticket.www/cn/EDM/360buy-card.htm
	 * 
	 * */
	public static String convertURL(String url) {
		String urlStr = url.replaceAll(" ", "");
		// 去锟斤拷http://前缀
		if((urlStr.indexOf("http://") == 0) || (urlStr.indexOf("HTTP://") == 0)) 
			urlStr = urlStr.substring(7);
		else if((urlStr.indexOf("https://") == 0) || (urlStr.indexOf("HTTPS://") == 0))
			urlStr = urlStr.substring(8);
		int slashPos = urlStr.indexOf("/");
		int wellPos = urlStr.indexOf("#");
		int portPos = urlStr.indexOf(":");
		String urlPost = null;
		String domainUrlPre = null;
		String urlPort = null;
		/**
		 * 锟斤拷式1锟斤拷www.sina.com.cn/cn/2012-10-15/...
		 * 锟斤拷式2:16.212.66.50:8080/news/...
		 * */
		if(slashPos != -1) {
			String str = urlStr.substring(0, slashPos);
			urlPost = urlStr.substring(slashPos); 
			// 锟叫端口猴拷
			if(str.indexOf(":") != -1) {
				int colonPos = str.indexOf(":");
				domainUrlPre = str.substring(0, colonPos);
				urlPort = str.substring(colonPos + 1); 
			}
			else
				domainUrlPre = str;
		}
		// news.sina.com.cn#锟斤拷锟街革拷式
		else if((wellPos != -1) && (slashPos == -1)) {
			String str = urlStr.substring(0, wellPos);
			urlPost = urlStr.substring(wellPos);
			// 锟叫端口猴拷
			if(str.indexOf(":") != -1) {
				int colonPos = str.indexOf(":");
				domainUrlPre = str.substring(0, colonPos);
				urlPort = str.substring(colonPos + 1);
			}
			else
				domainUrlPre = str;
		} else if(portPos != -1) {
			domainUrlPre = urlStr.substring(0, portPos);
			urlPort = urlStr.substring(portPos + 1);			
		}
		else
			domainUrlPre = urlStr;
		
		/**
		 * 锟叫讹拷锟角凤拷锟斤拷锟斤拷锟斤拷锟斤拷
		 * */
		String[] cols = domainUrlPre.split("\\.");
		boolean isNumericDomain = true;
		for(int i = 0; i < cols.length; i ++) {
			try {
			  Integer.parseInt(cols[i]);
			}
			catch(Exception e) {
				isNumericDomain = false;
				break;
			}
		}
		/**
		 * 锟斤拷转锟街凤拷锟斤拷锟斤拷
		 * */
		if(!isNumericDomain) {
			String str = "";
			for(int i = cols.length - 1; i >= 0; i --) {
				str = str + cols[i] + ".";
			}
			if(str.length() > 0) 
				str = str.substring(0, str.length() - 1);
			domainUrlPre = str;
		}
		
		/**
		 * 锟斤拷锟経RL
		 * */
		String returnUrl = domainUrlPre;
		if(urlPort != null)
			returnUrl = returnUrl + ":" + urlPort;
		if(urlPost != null)
			returnUrl = returnUrl + urlPost;
		
		return returnUrl;
	}
	
	/**
	 * IP to byte[4]
	 * 112.12.32.11
	 * */
	public static byte[] ip2bytes(String ip) {
		byte[] ips = new byte[4];
		
		String[] cols = ip.split("\\.");
		if((cols != null) && (cols.length == 4)) {
			for(int i = 0; i < 4; i ++) {
				int n = Integer.parseInt(cols[i]);
				ips[i] = (byte)n;
			}
		}
		else {
			for(int i = 0; i < 4; i ++)
				ips[i] = (byte)0;
		}
		return ips;
	}
	
	/**
	 * Datetime to byte[7]
	 * 2012-11-02 08:10:10
	 * */
	public static byte[] time2bytes(String time) {
		String ltime = convertDatetime(time);
		if(ltime.length() == 14) {
			byte[] times = new byte[7];
			for(int i = 0; i < 7; i ++) {
				int n = Integer.parseInt(ltime.substring(i * 2, (i + 1) * 2));
				times[i] = (byte)n;
			}
			return times;
		}
		else
			return null;
	}
}
