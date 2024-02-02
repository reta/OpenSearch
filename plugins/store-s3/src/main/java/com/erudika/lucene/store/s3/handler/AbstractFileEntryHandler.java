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
package com.erudika.lucene.store.s3.handler;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.store.s3.SocketAccess;

import java.io.IOException;

import com.erudika.lucene.store.s3.S3Directory;
import com.erudika.lucene.store.s3.S3FileEntrySettings;
import com.erudika.lucene.store.s3.S3StoreException;
import com.erudika.lucene.store.s3.index.S3IndexConfigurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base file entry handler that supports most of the file entry base operations.
 * <p>
 * Supports the creation of configurable <code>IndexInput</code> and <code>IndexOutput</code>, base on the
 * {@link S3FileEntrySettings#INDEX_INPUT_TYPE_SETTING} and {@link S3FileEntrySettings#INDEX_OUTPUT_TYPE_SETTING}.
 * <p>
 * Does not implement the deletion of files.
 *
 * @author kimchy
 */
public abstract class AbstractFileEntryHandler implements FileEntryHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractFileEntryHandler.class);

    protected S3Directory s3Directory;

    protected String bucket;

    @Override
    public void configure(final S3Directory s3Directory) {
        this.s3Directory = s3Directory;
        bucket = s3Directory.getBucket();
    }

    @Override
    public boolean fileExists(final String name) throws IOException {
        try {
            if (logger.isDebugEnabled()) {
                logger.info("fileExists({})", name);
            }
            SocketAccess.doPrivilegedVoid(() -> s3Directory.getS3().headObject(b -> b.bucket(bucket).key(s3Directory.getKey(name))));
            return true;
        } catch (AwsServiceException | SdkClientException e) {
            return false;
        }
    }

    @Override
    public long fileModified(final String name) throws IOException {
        try {
            if (logger.isDebugEnabled()) {
                logger.info("fileModified({})", name);
            }
            ResponseInputStream<GetObjectResponse> res = SocketAccess.doPrivileged(
                () -> s3Directory.getS3().getObject(b -> b.bucket(bucket).key(s3Directory.getKey(name)))
            );
            return res.response().lastModified().toEpochMilli();
        } catch (Exception e) {
            return 0L;
        }
    }

    @Override
    public void touchFile(final String name) throws IOException {
        try {
            if (logger.isDebugEnabled()) {
                logger.info("touchFile({})", name);
            }
            ResponseInputStream<GetObjectResponse> res = SocketAccess.doPrivileged(
                () -> s3Directory.getS3().getObject(b -> b.bucket(bucket).key(s3Directory.getKey(name)))
            );

            SocketAccess.doPrivilegedVoid(
                () -> s3Directory.getS3()
                    .putObject(
                        b -> b.bucket(bucket).key(s3Directory.getKey(name)),
                        RequestBody.fromInputStream(res, res.response().contentLength())
                    )
            );
        } catch (Exception e) {
            logger.error(null, e);
        }
    }

    @Override
    public void renameFile(final String from, final String to) throws IOException {
        try {
            if (logger.isDebugEnabled()) {
                logger.info("renameFile({}, {})", from, to);
            }
            SocketAccess.doPrivilegedVoid(() -> {
                s3Directory.getS3()
                    .copyObject(
                        b -> b.sourceBucket(bucket)
                            .sourceKey(s3Directory.getKey(from))
                            .destinationBucket(bucket)
                            .destinationKey(s3Directory.getKey(to))
                    );
                s3Directory.getFileSizes().put(s3Directory.getKey(to), s3Directory.getFileSizes().remove(s3Directory.getKey(from)));
            });
            deleteFile(from);
        } catch (Exception e) {
            logger.error(null, e);
        }
    }

    @Override
    public long fileLength(final String name) throws IOException {
        try {
            if (logger.isDebugEnabled()) {
                logger.info("fileLength({})", name);
            }

            return SocketAccess.doPrivileged(
                () -> s3Directory.getFileSizes()
                    .computeIfAbsent(
                        name,
                        n -> s3Directory.getS3().getObject(b -> b.bucket(bucket).key(s3Directory.getKey(name))).response().contentLength()
                    )
            );
        } catch (Exception e) {
            logger.error(null, e);
            return 0L;
        }
    }

    @Override
    public IndexInput openInput(final String name) throws IOException {
        IndexInput indexInput;
        final S3FileEntrySettings settings = s3Directory.getSettings().getFileEntrySettings(name);
        try {
            final Class<?> inputClass = settings.getSettingAsClass(S3FileEntrySettings.INDEX_INPUT_TYPE_SETTING, null);
            indexInput = SocketAccess.doPrivilegedIOException(() -> (IndexInput) inputClass.getConstructor(String.class).newInstance(name));
        } catch (final Exception e) {
            throw new S3StoreException(
                "Failed to create indexInput instance [" + settings.getSetting(S3FileEntrySettings.INDEX_INPUT_TYPE_SETTING) + "]",
                e
            );
        }
        ((S3IndexConfigurable) indexInput).configure(name, s3Directory, settings);
        return indexInput;
    }

    @Override
    public IndexOutput createOutput(final String name) throws IOException {
        IndexOutput indexOutput;
        final S3FileEntrySettings settings = s3Directory.getSettings().getFileEntrySettings(name);
        try {
            final Class<?> inputClass = settings.getSettingAsClass(S3FileEntrySettings.INDEX_OUTPUT_TYPE_SETTING, null);
            indexOutput = (IndexOutput) inputClass.getConstructor(String.class).newInstance(name);
        } catch (final Exception e) {
            throw new S3StoreException(
                "Failed to create indexOutput instance [" + settings.getSetting(S3FileEntrySettings.INDEX_OUTPUT_TYPE_SETTING) + "]",
                e
            );
        }
        ((S3IndexConfigurable) indexOutput).configure(name, s3Directory, settings);
        return indexOutput;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
