/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.s3;

import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.IndexStorePlugin.DirectoryFactory;
import org.opensearch.plugins.Plugin;
import org.opensearch.store.s3.S3DirectoryFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * S3 based store plugin
 */
public class S3StorePlugin extends Plugin implements IndexStorePlugin {
    /**
     * Create new S3 based store plugin
     */
    public S3StorePlugin() {}

    /**
     * The {@link DirectoryFactory} mappings for this plugin. When an index is created the store type setting
     * {@link org.opensearch.index.IndexModule#INDEX_STORE_TYPE_SETTING} on the index will be examined and either use the default or a
     * built-in type, or looked up among all the directory factories from {@link IndexStorePlugin} plugins.
     *
     * @return a map from store type to an directory factory
     */
    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        final Map<String, DirectoryFactory> indexStoreFactories = new HashMap<>(2);
        indexStoreFactories.put("s3", new S3DirectoryFactory());
        return Collections.unmodifiableMap(indexStoreFactories);
    }
}
