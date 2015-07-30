package iitb.CSAW.Query.Parser;

import iitb.CSAW.Query.AtomQuery;
import iitb.CSAW.Query.ContextQuery;
import iitb.CSAW.Query.MatcherQuery;
import iitb.CSAW.Query.PhraseQuery;
import iitb.CSAW.Query.RootQuery;
import iitb.CSAW.Query.TokenLiteralQuery;

import java.io.StringReader;

/**
 * Depends on {@link CSAWQueryGrammar} for parsing query strings to generate {@link RootQuery}
 * @see {@link CSAWQueryGrammar.jj} for the grammar. Compile the jj file to generate the java code for
 * 	the parser
 * @author devshree
 * @since 15 Nov 2010
 */
public class QueryParser
{

    static QueryParser obj;
    
    static CSAWQueryGrammar cqg;
    
    private QueryParser()
    {
	cqg = new CSAWQueryGrammar(new StringReader(""));
    }
    
    public static synchronized QueryParser getInstance()
    {
	if(obj == null)
	    obj = new QueryParser();
	return obj;
    }

    /**
     * @param p_query query string to parse
     * @return {@link RootQuery} object created. 
     *         null if query does not satisfy the constraints in {@link #checkConstraints(RootQuery)}
     */
    public RootQuery parseString(String p_query)
	{
		RootQuery parsedQueryObject=null;
		synchronized (obj)
		{
			CSAWQueryGrammar.ReInit(new StringReader(p_query));
			try
			{
			    parsedQueryObject=CSAWQueryGrammar.parse();
			    checkConstraints(parsedQueryObject);
			}
			catch (ParseException e)
			{
			    System.out.println(e.getMessage());
			    parsedQueryObject = null;
			}
			catch(IllegalArgumentException iae)
			{
			    System.out.println("Query does not satisfy constraints. "+iae.getMessage());
			    parsedQueryObject = null;
			}
		}
		return parsedQueryObject;
	}

    private static void checkConstraints(RootQuery rq)
    {
	if(rq.contexts.size() > 1)
	    throw new IllegalArgumentException("Number of contexts more than 1. This is not supported currently.");
	    
	for(ContextQuery cq: rq.contexts)
	    for(MatcherQuery mq: cq.matchers)
		if(mq instanceof PhraseQuery)
		    for(AtomQuery aq: ((PhraseQuery)mq).atoms)
			if(!(aq instanceof TokenLiteralQuery))
			    throw new IllegalArgumentException("Only tokens are allowed in phrases");
    }
    
    public static void main(String[] args) throws ParseException
    {
	String[] queries = { "uw(5 +[physicist] play -violin)", 
		             "uw(14 +[Tennis_player] beat +{Federer} )", 
			     "uw (10 +[Tennis_players] +{Roger_Federer} +final +\"Grand Slam\")",
		             "uw(5 -[physicist] -violin)",
		             "uw(5 \"[physicist] violin\")",
		             "uw(5 \"{physicist} violin\")",
		             "uw(14 +[Tennis_player] beat +{Federer}) ow(14 +[Tennis_player] beat +{Nadal} ) ",
	
	};
	QueryParser cqp = QueryParser.getInstance();
	for(String q: queries)
	    System.out.println(cqp.parseString(q));
	
    }

}
