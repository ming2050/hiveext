package com.bitauto.udf;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Intersection extends UDF {
	//text1="[1,2,3,4]"
	//text2="1,2,3,4"
	public Text evaluate(final Text text1,final Text text2){
		if(null == text1||null == text2){
			return null;
		}
		//A
		String t1 = text1.toString();
		t1 = t1.substring(1, t1.length()-1);
		String[] ts1 = t1.split(",");
		Set set1 = new HashSet<String>();
		for (String s : ts1) {
			set1.add(s);
		}
		//B
		String t2 = text2.toString();
		String[] ts2 = t2.split(",");
		Set set2 = new HashSet<String>();
		for (String s : ts2) {
			set2.add(s);
		}
		//A intersection B
		set1.retainAll(set2);
		System.out.println(set1);
		if(set1.isEmpty()) return null;
		else return new Text(set1.toString());
	}
	public static void main(String[] args) {
		Intersection intsec = new Intersection();
		intsec.evaluate(new Text("[1,2,3]"), new Text("12,1"));
	}
}
