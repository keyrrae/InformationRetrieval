package edu.ucsb.cs.cs293s.solrplugin;

/**
 * Created by xuanwang on 1/18/17.
 */

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SyntaxError;


public class MyQParserPlugin extends QParserPlugin {

    @Override
    public void init(NamedList args) {
        SolrParams params = SolrParams.toSolrParams(args);
        // handle configuration parameters
        // passed through solrconfig.xml
    }

    @Override
    public QParser createParser(String qstr,
                                SolrParams localParams, SolrParams params, SolrQueryRequest req) {

        return new MyParser(qstr, localParams, params, req);
    }

    private static class MyParser extends QParser {

        private Query innerQuery;

        public MyParser(String qstr, SolrParams localParams,
                        SolrParams params, SolrQueryRequest req) {
            super(qstr, localParams, params, req);
            try {
                QParser parser = getParser(qstr, "lucene", getReq());
                this.innerQuery = parser.parse();
            } catch (SyntaxError ex) {
                throw new RuntimeException("error parsing query", ex);
            }
        }

        @Override
        public Query parse() throws SyntaxError {
            return new MyQuery(innerQuery);
        }
    }
}




