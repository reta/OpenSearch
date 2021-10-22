/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.transform;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.gradle.api.logging.Logging;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public abstract class SymbolicLinkDisregardingUntarTransform extends SymbolicLinkAwareUntarTransform {
    @Override
    public void unpack(File tarFile, File targetDir) throws IOException {
        Logging.getLogger(SymbolicLinkDisregardingUntarTransform.class)
            .info("Unpacking " + tarFile.getName() + " using " + SymbolicLinkDisregardingUntarTransform.class.getSimpleName() + ".");
        super.unpack(tarFile, targetDir);
    }

    @Override
    protected void onSymbolicLink(TarArchiveEntry entry, Path destination) throws IOException {
        // See please https://bugs.openjdk.java.net/browse/JDK-8218418
        Logging.getLogger(SymbolicLinkDisregardingUntarTransform.class)
            .info("Ignoring symbolic link " + entry.getLinkName() + " to " + destination);
    }
}
