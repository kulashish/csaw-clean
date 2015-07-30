package iitb.CSAW.EntityRank.InexTrec;

import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.EntityRank.PropertyKeys;
import iitb.CSAW.Query.ContextQuery;
import iitb.CSAW.Query.EntityLiteralQuery;
import iitb.CSAW.Query.IQueryVisitor;
import iitb.CSAW.Query.MatcherQuery;
import iitb.CSAW.Query.PhraseQuery;
import iitb.CSAW.Query.RootQuery;
import iitb.CSAW.Query.TokenLiteralQuery;
import iitb.CSAW.Query.TypeBindingQuery;
import iitb.CSAW.Query.MatcherQuery.Exist;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.lang.MutableString;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Specific to single entity type binding queries.
 * @author soumen
 */
public class QueryWithAnswers {
	/**
	 * Test harness
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		final Config config = new Config(args[0], args[1]);
		HashMap<String, QueryWithAnswers> qwas = new HashMap<String, QueryWithAnswers>();
		QueryWithAnswers.load(config, qwas);
//		for (QueryWithAnswers qwa : qwas.values()) {
//			System.out.println(qwa.posEntNames.size() + "," + qwa.negEntNames.size() + " " + qwa.bindingType() + " " + qwa.rootQuery);
//		}
		qwas.clear();
		QueryWithAnswers.loadFromMsExcel(new File(config.getString(csawXlsKey)), config.getInt(PropertyKeys.windowKey), qwas);
	}

	public static final String csawXlsKey = "CsawQueryFile";
	private static final Logger logger = Logger.getLogger(QueryWithAnswers.class);
	private static final String querySheetName = "Queries-1.0", posEntSheetName = "PosEnts-1.0", negEntSheetName = "NegEnts";
	
	static final String queryCsv = "Queries.csv", posEntCsv = "PosEnts.csv", negEntCsv = "NegEnts.csv";

	/** Change these if the spreadsheet first row changes. */
	static final String queryIdColName = "QueryID", doIncludeColName = "Include?",
	rawColName = "NaturalQuery", yagoColName = "YagoAtype", selBeginColName = "Selector/s",
	entColName = "Ent";

	public final RootQuery rootQuery;
	/** Correct and incorrect entities, looked up in {@link ACatalog} for IDs. No spaces. */
	public final ArrayList<String> posEntNames = new ArrayList<String>(), negEntNames = new ArrayList<String>();

	QueryWithAnswers(RootQuery rootQuery) {
		this.rootQuery = rootQuery;
	}
	
	/**
	 * Exporting and maintaining multiple separate CSV files is tedious.
	 * Direct loading from single MSExcel file exported from Google doc.
	 * @param csawXlsFile
	 * @param cqa
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void loadFromMsExcel(File csawXlsFile, int width, HashMap<String, QueryWithAnswers> cqa) throws FileNotFoundException, IOException {
		loadQueriesFromMsExcel(csawXlsFile, width, cqa);
		loadEntsFromMsExcel(csawXlsFile, cqa, posEntSheetName);
		loadEntsFromMsExcel(csawXlsFile, cqa, negEntSheetName);
	}
	
	public static void loadFromMsExcel(Config conf, int width, HashMap<String, QueryWithAnswers> cqa) throws FileNotFoundException, IOException {
		loadFromMsExcel(new File(conf.getString(QueryWithAnswers.csawXlsKey)), width, cqa);
	}
	
	private static void loadQueriesFromMsExcel(File csawXlsFile, int width, HashMap<String, QueryWithAnswers> cqa) throws FileNotFoundException, IOException {
		HSSFWorkbook wb = new HSSFWorkbook(new FileInputStream(csawXlsFile));
		HSSFSheet querySheet = wb.getSheet(querySheetName);
		ReferenceArrayList<String> queryRowHeaders = new ReferenceArrayList<String>();
		{
			HSSFRow row = querySheet.getRow(0);
			short minColIx = row.getFirstCellNum(), maxColIx = row.getLastCellNum();
			queryRowHeaders.size(maxColIx);
			for(short colIx=minColIx; colIx<maxColIx; colIx++) {
				HSSFCell cell = row.getCell(colIx);
				if(cell == null) continue;
				queryRowHeaders.set(colIx, cell.getRichStringCellValue().getString());
			}
		}
		int minRowIx = querySheet.getFirstRowNum(), maxRowIx = querySheet.getLastRowNum(), nQuery = 0;
		for (int rowIx = minRowIx + 1; rowIx < maxRowIx; ++rowIx) {
			HSSFRow row = querySheet.getRow(rowIx);
			if (row == null) continue;
			short minColIx = row.getFirstCellNum(), maxColIx = row.getLastCellNum();
			String queryId = null, yagoAtype = null;
			boolean doInclude = false;
			ContextQuery csawContext = new ContextQuery("c1", ContextQuery.Window.unordered, width);
			for (short colIx = minColIx; colIx < maxColIx; ++colIx) {
				if (queryIdColName.equals(queryRowHeaders.get(colIx))) {
					String cellText = row.getCell(colIx).getRichStringCellValue().getString().trim();
					queryId = cellText;
				}
				else if (doIncludeColName.equals(queryRowHeaders.get(colIx))) {
					HSSFCell cell = row.getCell(colIx);
					if (cell != null && cell.getCellType() == HSSFCell.CELL_TYPE_NUMERIC && cell.getNumericCellValue() == 1d) {
						doInclude = true;
					}
				}
				else if (yagoColName.equals(queryRowHeaders.get(colIx))) {
					String cellText = row.getCell(colIx).getRichStringCellValue().getString().trim();
					yagoAtype = cellText;
				}
				else if (selBeginColName.equals(queryRowHeaders.get(colIx))) {
					HSSFCell cell = row.getCell(colIx);
					String cellText = null;
					switch (cell.getCellType()) {
					case HSSFCell.CELL_TYPE_NUMERIC:
						cellText = String.format("%g", cell.getNumericCellValue()); 
						break;
					case HSSFCell.CELL_TYPE_STRING:
						cellText = cell.getRichStringCellValue().getString().trim();
						break;
					}
					if (cellText != null && !cellText.isEmpty()) {
						MatcherQuery matcher = getMatcher(cellText);
						if (matcher != null) {
							csawContext.matchers.add(matcher);
						}
					}
				}
			}
			if (doInclude) {
				++nQuery;
				TypeBindingQuery csawEntityBinding = new TypeBindingQuery("v1", yagoAtype);
				csawContext.matchers.add(csawEntityBinding);
				RootQuery csawQuery = new RootQuery(queryId);
				csawQuery.contexts.add(csawContext);
				cqa.put(queryId, new QueryWithAnswers(csawQuery));
			}
		}
		logger.info("Loaded " + nQuery + " items from " + csawXlsFile.getCanonicalPath() + "/" + querySheetName);
	}
	
	private static void loadEntsFromMsExcel(File csawXlsFile, HashMap<String, QueryWithAnswers> qwas, String sheetName) throws FileNotFoundException, IOException {
		HSSFWorkbook wb = new HSSFWorkbook(new FileInputStream(csawXlsFile));
		HSSFSheet entSheet = wb.getSheet(sheetName);
		ReferenceArrayList<String> entRowHeaders = new ReferenceArrayList<String>();
		{
			HSSFRow row = entSheet.getRow(0);
			short minColIx = row.getFirstCellNum(), maxColIx = row.getLastCellNum();
			entRowHeaders.size(maxColIx);
			for(short colIx=minColIx; colIx<maxColIx; colIx++) {
				HSSFCell cell = row.getCell(colIx);
				if(cell == null) continue;
				entRowHeaders.set(colIx, cell.getRichStringCellValue().getString().trim());
			}
		}
		int minRowIx = entSheet.getFirstRowNum(), maxRowIx = entSheet.getLastRowNum(), nEnt = 0;
		for (int rowIx = minRowIx + 1; rowIx < maxRowIx; ++rowIx) {
			HSSFRow row = entSheet.getRow(rowIx);
			if (row == null) continue;
			short minColIx = row.getFirstCellNum(), maxColIx = row.getLastCellNum();
			String queryId = null, entName = null;
			for (short colIx = minColIx; colIx < maxColIx; ++colIx) {
				if (colIx >= entRowHeaders.size()) continue;
				HSSFCell cell = row.getCell(colIx);
				if (queryIdColName.equals(entRowHeaders.get(colIx))) {
					queryId = cell.getRichStringCellValue().getString().trim();
				}
				else if (entColName.equals(entRowHeaders.get(colIx))) {
					if (cell.getCellType() == HSSFCell.CELL_TYPE_STRING) {
						entName = cell.getRichStringCellValue().getString().trim();
					}
				}
			}
			if (queryId != null && !queryId.isEmpty() && entName != null && !entName.isEmpty()) {
				if (qwas.containsKey(queryId) && sheetName.equals(posEntSheetName)) {
					List<String> ents = qwas.get(queryId).posEntNames;
					if (!ents.contains(entName)) {
						ents.add(entName);
					}
				}
				if (qwas.containsKey(queryId) && sheetName.equals(negEntSheetName)) {
					List<String> ents = qwas.get(queryId).negEntNames;
					if (!ents.contains(entName)) {
						ents.add(entName);
					}
				}
				++nEnt;
			}
		}		
		logger.info("Loaded " + nEnt + " items from " + csawXlsFile.getCanonicalPath() + "/" + sheetName);
	}

	/**
	 * If there is exactly one {@link TypeBindingQuery} under {@link #rootQuery},
	 * return the type in it, otherwise throw {@link IllegalArgumentException}.
	 * @return
	 */
	public String bindingType() {
		final MutableString ans = new MutableString();
		rootQuery.visit(new IQueryVisitor() {
			@Override
			public void visit(MatcherQuery matcher) {
				if (matcher instanceof TypeBindingQuery) {
					if (ans.length() > 0) {
						throw new IllegalArgumentException("Multiple " + TypeBindingQuery.class + " nodes found in " + rootQuery);
					}
					final TypeBindingQuery tbq = (TypeBindingQuery) matcher;
					ans.replace(tbq.typeName);
				}
			}
		});
		if (ans.length() == 0) {
			throw new IllegalArgumentException("No " + TypeBindingQuery.class + " node found in " + rootQuery);
		}
		return ans.toString();
	}
	
	public static MatcherQuery getMatcher(String rawSelector) {
		if (rawSelector.isEmpty()) return null;
		final char firstChar = rawSelector.charAt(0);
		final Exist exist = firstChar == '+' ? Exist.must : firstChar == '-' ? Exist.not : Exist.may;
		if (firstChar == '+' || firstChar == '-') {
			rawSelector = rawSelector.substring(1);
		}
		final char nextChar = rawSelector.charAt(0);
		if (nextChar == '{') {
			return new EntityLiteralQuery(rawSelector.substring(1, rawSelector.length()-1), exist);
		}
		if (rawSelector.contains("\"") || rawSelector.matches(".*\\s.*")) { // phrase
			rawSelector = rawSelector.replace('"', ' ').trim();
			PhraseQuery phrase = new PhraseQuery(exist);
			for (String tokenOfPhrase : rawSelector.split("\\s+")) {
				phrase.atoms.add(new TokenLiteralQuery(tokenOfPhrase, exist));
			}
			return phrase;
		}
		else {
			return new TokenLiteralQuery(rawSelector, exist);
		}
	}
	
	/* Legacy code using CSV files. */
	
	public static void load(Config conf, HashMap<String, QueryWithAnswers> qwas) throws IOException {
		final File baseDir = new File(conf.getString(PropertyKeys.batchQueryDirKey));
		final int width = conf.getInt(PropertyKeys.windowKey);
		Logger logger = Logger.getLogger(QueryWithAnswers.class);
		loadQueries2(baseDir, logger, width, qwas);
		loadEnts2(baseDir, logger, posEntCsv, qwas);
		loadEnts2(baseDir, logger, negEntCsv, qwas);
	}
	
	private static void loadQueries2(File baseDir, Logger logger, int width, HashMap<String, QueryWithAnswers> ans) throws IOException {
		File csvFile = new File(baseDir, queryCsv);
		CSVReader csvr = new CSVReader(new FileReader(csvFile), ',');
		final String[] colNames = csvr.readNext();
		final Object2IntOpenHashMap<String> colNameToNum = new Object2IntOpenHashMap<String>();
		colNameToNum.defaultReturnValue(-1);
		for (int cx = 0; cx < colNames.length; ++cx) {
			colNameToNum.put(colNames[cx], cx);
		}
		logger.debug(colNameToNum);
		for (String line[] = null; (line = csvr.readNext()) != null; ) {
			if (line.length < colNameToNum.size()) continue;
			final String queryId = line[colNameToNum.getInt(queryIdColName)];
			final String doInclude = line[colNameToNum.getInt(doIncludeColName)];
			final String rawQuery = line[colNameToNum.getInt(rawColName)];
			final String yagoAtype = line[colNameToNum.getInt(yagoColName)];
			if (!doInclude.equals("y") && !doInclude.equals("1")) continue;
			RootQuery csawQuery = new RootQuery(queryId);
			ContextQuery csawContext = new ContextQuery("c1", ContextQuery.Window.unordered, width);
			csawQuery.contexts.add(csawContext);
			TypeBindingQuery csawEntityBinding = new TypeBindingQuery("v1", yagoAtype);
			csawContext.matchers.add(csawEntityBinding);
			for (int lx = colNameToNum.getInt(selBeginColName); lx < line.length; ++lx) {
				MatcherQuery matcher = getMatcher(line[lx].trim());
				if (matcher != null) {
					csawContext.matchers.add(matcher);
				}
			}
			ans.put(queryId, new QueryWithAnswers(csawQuery));
			logger.debug(rawQuery + "\n" + csawQuery);
		}
		csvr.close();
		logger.info("Loaded " + ans.size() + " queries from " + csvFile);
	}
	
	/**
	 * Warning, CSV field magic here.
	 * @param logger
	 * @param csvFile
	 * @param qidToEnts loads ents even if this query has been disqualified 
	 * @throws IOException 
	 */
	private static void loadEnts2(File baseDir, Logger logger, String csvName, HashMap<String, QueryWithAnswers> qwas) throws IOException {
		int nRows = 0;
		File csvFile = new File(baseDir, csvName);
		CSVReader csvr = new CSVReader(new FileReader(csvFile), ',');
		for (String line[] = null; (line = csvr.readNext()) != null; ) {
			if (line.length < 2) continue;
			final String queryID = line[0].trim(), ent = line[1].trim();
			if (qwas.containsKey(queryID) && csvName.equals(posEntCsv)) {
				List<String> ents = qwas.get(queryID).posEntNames;
				if (!ents.contains(ent)) {
					ents.add(ent);
				}
			}
			if (qwas.containsKey(queryID) && csvName.equals(negEntCsv)) {
				List<String> ents = qwas.get(queryID).negEntNames;
				if (!ents.contains(ent)) {
					ents.add(ent);
				}
			}
			++nRows;
		}
		logger.info("loaded " + nRows + " items from " + csvFile);
	}
}
