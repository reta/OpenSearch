/*
 * Copyright 2004-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.erudika.lucene.store.s3;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.startsWith;
import static com.erudika.lucene.store.s3.S3DirectoryGeneralOperationsITest.TEST_BUCKET2;
import static org.junit.Assert.assertThat;

public class S3DirectoryIndexSearchITest extends AbstractS3DirectoryITest {
    private Directory directory;

    @Before
    public void setUp() throws Exception {
        directory = new S3Directory(TEST_BUCKET2, "0");
        ((S3Directory) directory).create();
    }

    @After
    public void tearDown() throws Exception {
        directory.close();
        ((S3Directory) directory).delete();
    }

    @Test
    public void testSearch() throws IOException, ParseException {
        final int docs = 500;

        try (final IndexWriter iwriter = new IndexWriter(directory, getIndexWriterConfig())) {
            for (int doc = 0; doc < docs; ++doc) {
                final Document document = new Document();
                final String text = "This is the text to be indexed " + doc;
                document.add(new Field("fieldname", text, TextField.TYPE_STORED));
                iwriter.addDocument(document);

                if (((doc + 1) % 10) == 0) {
                    iwriter.commit();
                }
            }

            if (iwriter.hasUncommittedChanges()) {
                iwriter.commit();
            }

            if (iwriter.isOpen()) {
                iwriter.getDirectory().close();
            }

            iwriter.forceMerge(1, true);
        }

        // Now search the index:
        try (DirectoryReader ireader = DirectoryReader.open(directory)) {
            final IndexSearcher isearcher = new IndexSearcher(ireader);
            // Parse a simple query that searches for "text":

            final QueryParser parser = new QueryParser("fieldname", analyzer);
            final Query query = parser.parse("text");
            final ScoreDoc[] hits = isearcher.search(query, 1000).scoreDocs;
            Assert.assertEquals(500, hits.length);
            // Iterate through the results:
            for (final ScoreDoc hit : hits) {
                final Document hitDoc = isearcher.doc(hit.doc);
                assertThat(hitDoc.get("fieldname"), startsWith("This is the text to be indexed "));
            }
        }
    }

    private IndexWriterConfig getIndexWriterConfig() {
        final IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(OpenMode.CREATE_OR_APPEND);
        return config;
    }
}
