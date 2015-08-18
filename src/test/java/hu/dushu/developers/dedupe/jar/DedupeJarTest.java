package hu.dushu.developers.dedupe.jar;

import org.apache.commons.codec.EncoderException;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by frren on 8/18/2015.
 */
public class DedupeJarTest {

	@Test
	public void test() throws IOException, EncoderException {

		DedupeJar dedupeJar = new DedupeJar();

		dedupeJar.clear();

		dedupeJar.refresh();

//		Map<String, Integer> map = dedupeJar.listJarsWithoutTag("approved");
//		Iterator<Map.Entry<String, Integer>> iterator = map.entrySet().iterator();
//		if (iterator.hasNext()) {
//			Map.Entry<String, Integer> entry = iterator.next();
//			dedupeJar.tag(entry.getKey(), entry.getValue(), "approved");
//		}
	}
}
