package iitb.CSAW.EntityRank.InexTrec;

import iitb.CSAW.Query.AtomQuery;
import iitb.CSAW.Query.ContextQuery;
import iitb.CSAW.Query.EntityLiteralQuery;
import iitb.CSAW.Query.MatcherQuery;
import iitb.CSAW.Query.PhraseQuery;
import iitb.CSAW.Query.RootQuery;
import iitb.CSAW.Query.TokenLiteralQuery;
import iitb.CSAW.Query.TypeBindingQuery;
import iitb.CSAW.Query.ContextQuery.Window;
import iitb.CSAW.Query.MatcherQuery.Exist;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

/**
 * Loads from telegraph query MSExcel exported from Google spreadsheet.
 * A {@link TelegraphQuery} is a set of {@link MatcherQuery}s.
 * Each element is either a {@link TokenLiteralQuery} or a
 * {@link PhraseQuery}. Therefore, we can reuse {@link RootQuery},
 * {@link ContextQuery} and {@link MatcherQuery}, except that there
 * will be no {@link TypeBindingQuery} or {@link EntityLiteralQuery}
 * used here.
 * 
 * @author soumen
 */
public class TelegraphQuery {
	public static final String telegraphXlsKey = "TelegraphQueryFile";
	static final Logger logger = Logger.getLogger(TelegraphQuery.class);
	
	public static HashMap<String, RootQuery> load(File telegraphXls) throws FileNotFoundException, IOException {
		HashMap<String, RootQuery> ans = new HashMap<String, RootQuery>();
		HSSFWorkbook wb = new HSSFWorkbook(new FileInputStream(telegraphXls));
		HSSFSheet sh = wb.getSheet("Queries-1.0");
		ReferenceArrayList<String> rowHeaders = new ReferenceArrayList<String>();
		{
			HSSFRow row = sh.getRow(0);
			short minColIx = row.getFirstCellNum(), maxColIx = row.getLastCellNum();
			rowHeaders.size(maxColIx);
			for(short colIx=minColIx; colIx<maxColIx; colIx++) {
				HSSFCell cell = row.getCell(colIx);
				if(cell == null) continue;
				rowHeaders.set(colIx, cell.getRichStringCellValue().getString());
			}
		}
		int width = 20;
		int minRowIx = sh.getFirstRowNum(), maxRowIx = sh.getLastRowNum();
		for (int rowIx = minRowIx + 1; rowIx < maxRowIx; ++rowIx) {
			HSSFRow row = sh.getRow(rowIx);
			if (row == null) continue;
			short minColIx = row.getFirstCellNum(), maxColIx = row.getLastCellNum();
			String queryId = null;
			for (short colIx = minColIx; colIx < maxColIx; ++colIx) {
				if ("QueryID".equals(rowHeaders.get(colIx))) {
					String cellText = row.getCell(colIx).getRichStringCellValue().getString().trim();
					queryId = cellText;
				}
				else if ("TelegraphQuery".equals(rowHeaders.get(colIx))) {
					logger.trace("[" + rowIx + "," + colIx + "] " + row.getCell(colIx));
					String cellText = row.getCell(colIx).getRichStringCellValue().getString().trim();
					if (!cellText.isEmpty()) {
						ContextQuery cq = new ContextQuery("c1", Window.unordered, width);
						Pattern pat = Pattern.compile("\\+?([^\\\"\\s]+|(\\\".*?\\\"))");
						Matcher mat = pat.matcher(cellText);
						while (mat.find()) {
							String clause = mat.group();
							logger.trace("\t[" + clause + "]");
							Exist exist = clause.startsWith("+")? Exist.must : Exist.may;
							if (clause.startsWith("+\"") || clause.startsWith("\"")) {
								String words[] = clause.split("[\\+\\s\\\"]+");
								PhraseQuery phraseQuery = new PhraseQuery(exist);
								for (String word : words) {
									if (word.trim().isEmpty()) continue;
									TokenLiteralQuery tlQuery = new TokenLiteralQuery(word, exist);
									phraseQuery.atoms.add(tlQuery);
								}
								cq.matchers.add(phraseQuery);
							}
							else { // single token, possibly must
								String tokenText = clause.startsWith("+")? clause.substring(1) : clause;
								TokenLiteralQuery tlQuery = new TokenLiteralQuery(tokenText, exist);
								cq.matchers.add(tlQuery);
							}
						}
						RootQuery rq = new RootQuery(queryId);
						rq.contexts.add(cq);
						logger.trace(rq);
						ans.put(queryId, rq);
					}
				}
			}
		}
		return ans;
	}
	
	/**
	 * @param rootQuery a telegraphic query
	 * @return break up phrases but keep words in the same order; do not stem
	 */
	public static ArrayList<String> telegraphQueryToTokenLiterals(RootQuery rootQuery) {
		ArrayList<String> ans = new ArrayList<String>();
		for (MatcherQuery mq : rootQuery.contexts.pop().matchers) {
			if (mq instanceof TokenLiteralQuery) {
				TokenLiteralQuery tlq = (TokenLiteralQuery) mq;
				ans.add(tlq.tokenText);
			}
			else if (mq instanceof PhraseQuery) {
				PhraseQuery pq = (PhraseQuery) mq;
				for (AtomQuery aq : pq.atoms) {
					if (aq instanceof TokenLiteralQuery) {
						TokenLiteralQuery tlq = (TokenLiteralQuery) aq;
						ans.add(tlq.tokenText);
					}
				}
			}
		}
		return ans;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		TelegraphQuery.load(new File(args[0]));
	}
}
