package edu.ucsb.cs.cs293s.solrplugin;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.CustomScoreProvider;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.search.Query;


import java.io.IOException;
import java.util.List;

/**
 * Created by xuanwang on 1/18/17.
 */
public class MyQuery extends CustomScoreQuery {

    public MyQuery(Query subQuery) {
        super(subQuery);
    }

    @Override
    protected CustomScoreProvider getCustomScoreProvider(
            LeafReaderContext context) throws IOException {
        return new MyScoreProvider(context);
    }

    class MyScoreProvider extends CustomScoreProvider {

        public MyScoreProvider(LeafReaderContext context) {
            super(context);
        }

        @Override
        public float customScore(int doc, float subQueryScore,
                                 float valSrcScore) throws IOException {
            return customScore(doc, subQueryScore, new float[]{valSrcScore});
        }

        @Override
        public float customScore(int doc, float subQueryScore,
                                 float[] valSrcScores) throws IOException {
            // Method is called for every
            // matching document of the subQuery

            Document d = context.reader().document(doc);
            // plugin external score calculation based on the fields...
            List fields = d.getFields();
            // and return the custom score
            float score = 1.0f;
            return score;

        }
    }
}

