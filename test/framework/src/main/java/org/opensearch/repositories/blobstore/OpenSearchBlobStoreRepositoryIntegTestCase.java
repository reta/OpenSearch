/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.repositories.blobstore;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.SetOnce;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.io.Streams;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.snapshots.SnapshotMissingException;
import org.opensearch.snapshots.SnapshotRestoreException;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for {@link BlobStoreRepository} implementations.
 */
public abstract class OpenSearchBlobStoreRepositoryIntegTestCase extends OpenSearchIntegTestCase {

    public static RepositoryData getRepositoryData(Repository repository) {
        return PlainActionFuture.get(repository::getRepositoryData);
    }

    protected abstract String repositoryType();

    protected Settings repositorySettings() {
        final boolean compress = randomBoolean();
        final Settings.Builder builder = Settings.builder();
        builder.put("compress", compress);
        if (compress) {
            builder.put("compression_type", randomFrom(CompressorRegistry.registeredCompressors().keySet()));
        }
        return builder.build();
    }

    protected final String createRepository(final String name) {
        return createRepository(name, repositorySettings());
    }

    protected final String createRepository(final String name, final Settings settings) {
        final boolean verify = randomBoolean();

        logger.debug("-->  creating repository [name: {}, verify: {}, settings: {}]", name, verify, settings);
        OpenSearchIntegTestCase.putRepository(client().admin().cluster(), name, repositoryType(), verify, Settings.builder().put(settings));

        internalCluster().getDataOrClusterManagerNodeInstances(RepositoriesService.class).forEach(repositories -> {
            assertThat(repositories.repository(name), notNullValue());
            assertThat(repositories.repository(name), instanceOf(BlobStoreRepository.class));
            assertThat(repositories.repository(name).isReadOnly(), is(false));
            BlobStore blobStore = ((BlobStoreRepository) repositories.repository(name)).getBlobStore();
            assertThat("blob store has to be lazy initialized", blobStore, verify ? is(notNullValue()) : is(nullValue()));
        });

        return name;
    }

    
    public void testDeleteBlobs() throws IOException {
        try (BlobStore store = newBlobStore()) {
            final List<String> blobNames = Arrays.asList("foobar", "barfoo");
            final BlobContainer container = store.blobContainer(new BlobPath());
            container.deleteBlobsIgnoringIfNotExists(blobNames); // does not raise when blobs don't exist
            byte[] data = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
            final BytesArray bytesArray = new BytesArray(data);
            for (String blobName : blobNames) {
                writeBlob(container, blobName, bytesArray, randomBoolean());
            }
            assertEquals(container.listBlobs().size(), 2);
            container.deleteBlobsIgnoringIfNotExists(blobNames);
            assertTrue(container.listBlobs().isEmpty());
            container.deleteBlobsIgnoringIfNotExists(blobNames); // does not raise when blobs don't exist
        }
    }

    public static void writeBlob(
        final BlobContainer container,
        final String blobName,
        final BytesArray bytesArray,
        boolean failIfAlreadyExists
    ) throws IOException {
        try (InputStream stream = bytesArray.streamInput()) {
            if (randomBoolean()) {
                container.writeBlob(blobName, stream, bytesArray.length(), failIfAlreadyExists);
            } else {
                container.writeBlobAtomic(blobName, stream, bytesArray.length(), failIfAlreadyExists);
            }
        }
    }

    public static byte[] writeRandomBlob(BlobContainer container, String name, int length) throws IOException {
        byte[] data = randomBytes(length);
        writeBlob(container, name, new BytesArray(data));
        return data;
    }

    public static byte[] readBlobFully(BlobContainer container, String name, int length) throws IOException {
        byte[] data = new byte[length];
        try (InputStream inputStream = container.readBlob(name)) {
            assertThat(Streams.readFully(inputStream, data), CoreMatchers.equalTo(length));
            assertThat(inputStream.read(), CoreMatchers.equalTo(-1));
        }
        return data;
    }

    public static byte[] randomBytes(int length) {
        byte[] data = new byte[length];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) randomInt();
        }
        return data;
    }

    protected static void writeBlob(BlobContainer container, String blobName, BytesArray bytesArray) throws IOException {
        try (InputStream stream = bytesArray.streamInput()) {
            container.writeBlob(blobName, stream, bytesArray.length(), true);
        }
    }

    protected BlobStore newBlobStore() {
        final String repository = createRepository(randomName());
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) internalCluster().getClusterManagerNodeInstance(
            RepositoriesService.class
        ).repository(repository);
        return PlainActionFuture.get(
            f -> blobStoreRepository.threadPool().generic().execute(ActionRunnable.supply(f, blobStoreRepository::blobStore))
        );
    }

    public void testIndicesDeletedFromRepository() throws Exception {
        final String repoName = createRepository("test-repo-" + randomAlphaOfLength(8));
        Client client = client();
        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 20; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "doc", Integer.toString(i), "foo", "baz" + i);
            index("test-idx-3", "doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();

        logger.info("--> take a snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(repoName, "test-snap")
            .setWaitForCompletion(true)
            .get();
        assertEquals(createSnapshotResponse.getSnapshotInfo().successfulShards(), createSnapshotResponse.getSnapshotInfo().totalShards());

        logger.info("--> indexing more data");
        for (int i = 20; i < 40; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "doc", Integer.toString(i), "foo", "baz" + i);
            index("test-idx-3", "doc", Integer.toString(i), "foo", "baz" + i);
        }

        logger.info("--> take another snapshot with only 2 of the 3 indices");
        createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(repoName, "test-snap2")
            .setWaitForCompletion(true)
            .setIndices("test-idx-1", "test-idx-2")
            .get();
        assertEquals(createSnapshotResponse.getSnapshotInfo().successfulShards(), createSnapshotResponse.getSnapshotInfo().totalShards());

        logger.info("--> delete a snapshot");
        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repoName, "test-snap").get());

        logger.info("--> verify index folder deleted from blob container");
        RepositoriesService repositoriesSvc = internalCluster().getInstance(
            RepositoriesService.class,
            internalCluster().getClusterManagerName()
        );
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, internalCluster().getClusterManagerName());
        BlobStoreRepository repository = (BlobStoreRepository) repositoriesSvc.repository(repoName);

        final SetOnce<BlobContainer> indicesBlobContainer = new SetOnce<>();
        final PlainActionFuture<RepositoryData> repositoryData = PlainActionFuture.newFuture();
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            indicesBlobContainer.set(repository.blobStore().blobContainer(repository.basePath().add("indices")));
            repository.getRepositoryData(repositoryData);
        });

        for (IndexId indexId : repositoryData.actionGet().getIndices().values()) {
            if (indexId.getName().equals("test-idx-3")) {
                assertFalse(indicesBlobContainer.get().blobExists(indexId.getId())); // deleted index
            }
        }

        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repoName, "test-snap2").get());
    }

    protected void addRandomDocuments(String name, int numDocs) throws InterruptedException {
        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            indexRequestBuilders[i] = client().prepareIndex(name)
                .setId(Integer.toString(i))
                .setRouting(randomAlphaOfLength(randomIntBetween(1, 10)))
                .setSource("field", "value");
        }
        indexRandom(true, indexRequestBuilders);
    }

    private String[] generateRandomNames(int num) {
        Set<String> names = new HashSet<>();
        for (int i = 0; i < num; i++) {
            String name;
            do {
                name = randomName();
            } while (names.contains(name));
            names.add(name);
        }
        return names.toArray(new String[num]);
    }

    protected static void assertSuccessfulSnapshot(CreateSnapshotRequestBuilder requestBuilder) {
        CreateSnapshotResponse response = requestBuilder.get();
        assertSuccessfulSnapshot(response);
    }

    private static void assertSuccessfulSnapshot(CreateSnapshotResponse response) {
        assertThat(response.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(response.getSnapshotInfo().successfulShards(), equalTo(response.getSnapshotInfo().totalShards()));
    }

    protected static void assertSuccessfulRestore(RestoreSnapshotRequestBuilder requestBuilder) {
        RestoreSnapshotResponse response = requestBuilder.get();
        assertSuccessfulRestore(response);
    }

    private static void assertSuccessfulRestore(RestoreSnapshotResponse response) {
        assertThat(response.getRestoreInfo().successfulShards(), greaterThan(0));
        assertThat(response.getRestoreInfo().successfulShards(), equalTo(response.getRestoreInfo().totalShards()));
    }

    protected static String randomName() {
        return randomAlphaOfLength(randomIntBetween(5, 10)).toLowerCase(Locale.ROOT);
    }
}
