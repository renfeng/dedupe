package hu.dushu.developers.dedupe.jar;

import org.apache.commons.codec.EncoderException;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

/**
 * Created by frren on 8/18/2015.
 */
public class DedupeJarTest {

	final DedupeJar dedupeJar = new DedupeJar();

	@Test
	public void test() throws IOException, EncoderException {

//		dedupeJar.clear();

		dedupeJar.refresh();
	}

	@Test
	public void testListJarsWithoutTagApproved() throws IOException, EncoderException {
		SortedMap<String, Integer> map = dedupeJar.listJarsWithoutTag("approved");
		Iterator<Map.Entry<String, Integer>> iterator = map.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, Integer> entry = iterator.next();
			String jar = entry.getKey();
			Integer count = entry.getValue();
//			dedupeJar.tag(jar, count, "approved");
			System.out.println(jar);
		}
	}
}
