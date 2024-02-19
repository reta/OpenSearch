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
package com.erudika.lucene.store.s3.index;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import org.apache.lucene.store.IndexInput;
import org.opensearch.store.s3.SocketAccess;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.erudika.lucene.store.s3.S3Directory;
import com.erudika.lucene.store.s3.S3FileEntrySettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An <code>IndexInput</code> implementation, that for every buffer refill will go and fetch the data from the database.
 *
 * @author kimchy
 */
public class FetchOnBufferReadS3IndexInput extends S3BufferedIndexInput {

    private static final Logger logger = LoggerFactory.getLogger(FetchOnBufferReadS3IndexInput.class);

    private String name;

    // lazy intialize the length
    private long totalLength = -1;

    private long position = 0;

    private S3Directory s3Directory;

    public FetchOnBufferReadS3IndexInput(final String name) {
        super(name);
    }

    @Override
    public void configure(final String name, final S3Directory s3Directory, final S3FileEntrySettings settings) throws IOException {
        super.configure(name, s3Directory, settings);
        this.s3Directory = s3Directory;
        this.name = name;
    }

    @Override
    protected void readInternal(ByteBuffer buffer) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.info("readInternal({})", name);
        }

        try (
            ResponseInputStream<GetObjectResponse> res = SocketAccess.doPrivileged(
                () -> s3Directory.getS3().getObject(bd -> bd.bucket(s3Directory.getBucket()).key(s3Directory.getKey(name)))
            )
        ) {
            synchronized (this) {
                if (totalLength == -1) {
                    totalLength = res.response().contentLength();
                }
            }

            readInternal(res, buffer);
        }
    }

    private synchronized void readInternal(final ResponseInputStream<GetObjectResponse> res, ByteBuffer b) throws IOException {
        final long curPos = getFilePointer();
        if (curPos != position) {
            position = curPos;
        }
        res.skip(position);

        if (position + b.remaining() > totalLength) {
            throw new EOFException("read past EOF: " + this);
        }

        try {
            while (b.hasRemaining()) {
                final int toRead = Math.min(getBufferSize(), b.remaining());
                final int i = res.read(b.array(), b.position(), toRead);

                if (i < 0) {
                    // be defensive here, even though we checked before hand, something could have changed
                    throw new EOFException(
                        "read past EOF: "
                            + this
                            + " off: "
                            + b.position()
                            + " len: "
                            + b.remaining()
                            + " chunkLen: "
                            + toRead
                            + " end: "
                            + totalLength
                    );
                }

                b.position(b.position() + i);
                position += i;
            }
        } catch (IOException ioe) {
            throw new IOException(ioe.getMessage() + ": " + this, ioe);
        }

    }

    @Override
    protected void seekInternal(final long pos) throws IOException {
        position = pos;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public synchronized long length() {
        if (totalLength == -1) {
            try {
                totalLength = s3Directory.fileLength(name);
            } catch (final IOException e) {
                // do nothing here for now, much better for performance
            }
        }
        return totalLength;
    }

    @Override
    public IndexInput slice(final String sliceDescription, final long offset, final long length) throws IOException {
        return new SlicedIndexInput(sliceDescription, this, offset, length);
    }
}
