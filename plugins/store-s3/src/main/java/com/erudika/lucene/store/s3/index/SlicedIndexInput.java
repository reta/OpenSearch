/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.erudika.lucene.store.s3.index;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Implementation of an IndexInput that reads from a portion of a file.
 */
final class SlicedIndexInput extends BufferedIndexInput {
    private IndexInput base;
    private long fileOffset;
    private long length;

    SlicedIndexInput(final String sliceDescription, final IndexInput base, final long offset, final long length) {
        super(
            sliceDescription == null ? base.toString() : base.toString() + " [slice=" + sliceDescription + "]",
            BufferedIndexInput.BUFFER_SIZE
        );
        if (offset < 0 || length < 0 || offset + length > base.length()) {
            throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: " + base);
        }
        this.base = base.clone();
        this.fileOffset = offset;
        this.length = length;
    }

    @Override
    public SlicedIndexInput clone() {
        final SlicedIndexInput clone = (SlicedIndexInput) super.clone();
        clone.base = base.clone();
        clone.fileOffset = fileOffset;
        clone.length = length;
        return clone;
    }

    @Override
    protected void readInternal(ByteBuffer bb) throws IOException {
        long start = getFilePointer();
        if (start + bb.remaining() > length) {
            throw new EOFException("read past EOF: " + this);
        }
        base.seek(fileOffset + start);
        base.readBytes(bb.array(), bb.position(), bb.remaining());
        bb.position(bb.position() + bb.remaining());
    }

    @Override
    protected void seekInternal(final long pos) {}

    @Override
    public void close() throws IOException {
        base.close();
    }

    @Override
    public long length() {
        return length;
    }
}
