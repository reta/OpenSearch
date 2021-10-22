/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.transform;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

import static org.opensearch.gradle.util.PermissionUtils.chmod;

abstract class SymbolicLinkAwareUntarTransform implements UnpackTransform {
    private static final Path CURRENT_DIR_PATH = Paths.get(".");

    @Override
    public void unpack(File tarFile, File targetDir) throws IOException {
        Function<String, Path> pathModifier = pathResolver();

        try (
            FileInputStream fis = new FileInputStream(tarFile);
            GzipCompressorInputStream gzip = new GzipCompressorInputStream(fis);
            TarArchiveInputStream tar = new TarArchiveInputStream(gzip)
        ) {
            final Path destinationPath = targetDir.toPath();
            TarArchiveEntry entry = tar.getNextTarEntry();
            while (entry != null) {
                final Path relativePath = pathModifier.apply(entry.getName());
                if (relativePath == null || relativePath.getFileName().equals(CURRENT_DIR_PATH)) {
                    entry = tar.getNextTarEntry();
                    continue;
                }

                final Path destination = destinationPath.resolve(relativePath);
                final Path parent = destination.getParent();
                if (Files.exists(parent) == false) {
                    Files.createDirectories(parent);
                }
                if (entry.isDirectory()) {
                    onDirectory(destination);
                } else if (entry.isSymbolicLink()) {
                    onSymbolicLink(entry, destination);
                } else {
                    onFile(tar, destination);
                }
                if (entry.isSymbolicLink() == false) {
                    // check if the underlying file system supports POSIX permissions
                    chmod(destination, entry.getMode());
                }
                entry = tar.getNextTarEntry();
            }
        }

    }

    protected void onFile(TarArchiveInputStream tar, final Path destination) throws IOException, FileNotFoundException {
        // copy the file from the archive using a small buffer to avoid heaping
        Files.createFile(destination);
        try (FileOutputStream fos = new FileOutputStream(destination.toFile())) {
            tar.transferTo(fos);
        }
    }

    protected void onSymbolicLink(TarArchiveEntry entry, final Path destination) throws IOException {
        Files.createSymbolicLink(destination, Paths.get(entry.getLinkName()));
    }

    protected void onDirectory(final Path destination) throws IOException {
        Files.createDirectory(destination);
    }
}
