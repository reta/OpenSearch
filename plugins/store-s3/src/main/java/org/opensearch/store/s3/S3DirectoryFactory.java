/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.store.s3;

import org.apache.lucene.store.Directory;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.IndexStorePlugin.DirectoryFactory;

import java.io.IOException;

import com.erudika.lucene.store.s3.S3Directory;
import com.erudika.lucene.store.s3.S3DirectorySettings;

/**
 * S3 directory factory
 */
public class S3DirectoryFactory implements DirectoryFactory {
    /**
     * Create default S3 directory factory
     */
    public S3DirectoryFactory() {}

    /**
     * Creates a new directory per shard. This method is called once per shard on shard creation.
     * @param indexSettings the shards index settings
     * @param shardPath the path the shard is using
     * @return a new lucene directory instance
     * @throws IOException if an IOException occurs while opening the directory
     */
    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
        final S3DirectorySettings settings = new S3DirectorySettings();
        final S3Directory directory = new S3Directory(indexSettings.getIndex().getName(), settings);
        directory.create();
        return directory;
    }

}
