package com.bitauto.udf;

import java.nio.charset.Charset;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class Md5 extends UDF {
	public Text evaluate(final Text text){
		if(null == text){
			return text;
		}
		Hasher hasher = Hashing.md5().newHasher();
		hasher.putString(text.toString(), Charset.defaultCharset());
		String md5 = hasher.hash().toString();
		return new Text(md5);
	}
}
