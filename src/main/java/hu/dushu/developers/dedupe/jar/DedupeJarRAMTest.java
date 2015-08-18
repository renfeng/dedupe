package hu.dushu.developers.dedupe.jar;

import org.apache.commons.codec.EncoderException;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by frren on 8/18/2015.
 */
public class DedupeJarRAMTest {

	@Test
	public void test() throws IOException, EncoderException {

		DedupeJarRAM dedupeJarRAM = new DedupeJarRAM();

		dedupeJarRAM.clear();

		/*
		 * TODO incremental
		 */
		dedupeJarRAM.refresh();

		Map<String, Integer> map = dedupeJarRAM.listJarsWithoutTag("approved");
		Iterator<Map.Entry<String, Integer>> iterator = map.entrySet().iterator();
		if (iterator.hasNext()) {
			Map.Entry<String, Integer> entry = iterator.next();
			dedupeJarRAM.tag(entry.getKey(), entry.getValue(), "approved");
		}
	}
}
