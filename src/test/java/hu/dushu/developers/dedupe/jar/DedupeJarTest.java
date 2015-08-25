package hu.dushu.developers.dedupe.jar;

import org.apache.commons.codec.EncoderException;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
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
	public void testListJarsWithoutTag() throws IOException, EncoderException {
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

	@Test
	public void testListJarSpecificationsWithoutTag() throws IOException, EncoderException {

		FileOutputStream outputStream = new FileOutputStream("target/jar-specification-approval.xlsx");
		try {
			File template = new File("jar-specification-tag.template.xlsx");
			XSSFWorkbook wb = new XSSFWorkbook(new FileInputStream(template));
			try {
				XSSFSheet sheet = wb.getSheet("jar-specification-approval");

				int i = 0;
				SortedMap<String, Integer> map = dedupeJar.listSpecificationsWithoutTag("approved");
				Iterator<Map.Entry<String, Integer>> iterator = map.entrySet().iterator();
				while (iterator.hasNext()) {
					Map.Entry<String, Integer> entry = iterator.next();
					String title = entry.getKey();
					Integer count = entry.getValue();
					SortedMap<String, Integer> versionMap = dedupeJar.listSpecificationVersions(title);
					Iterator<Map.Entry<String, Integer>> versionIterator = versionMap.entrySet().iterator();
					while (versionIterator.hasNext()) {
						Map.Entry<String, Integer> versionEntry = versionIterator.next();
						String version = versionEntry.getKey();
						Integer versionCount = versionEntry.getValue();
						List<String> pathList = dedupeJar.listJarsBySpecification(title, version);
						for (String path : pathList) {
							int rowIndex = i + 1;
							int rowNumber = i + 2;
							XSSFRow row = sheet.createRow(rowIndex);
							int columnIndex = 0;
							{
								XSSFCell cell = row.createCell(columnIndex++);
								cell.setCellValue(title);
							}
							{
								XSSFCell cell = row.createCell(columnIndex++);
								cell.setCellValue(version);
							}
							{
								XSSFCell cell = row.createCell(columnIndex++);
								cell.setCellValue(path);
							}
							i++;
						}
					}
				}

				wb.write(outputStream);
			} finally {
			/*
			 * don't open from file directly,or the template will be modified
			 */
				wb.close();
			}
		} finally {
			outputStream.close();
		}
	}

	@Test
	public void testTag() throws IOException, EncoderException {
		dedupeJar.tagBySpecification("Commons Collections", "3.2.1", "approved");
	}
}
