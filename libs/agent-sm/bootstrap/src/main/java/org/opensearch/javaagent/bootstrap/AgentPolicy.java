/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent.bootstrap;

import java.lang.StackWalker.Option;
import java.lang.StackWalker.StackFrame;
import java.security.Permission;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Agent Policy
 */
@SuppressWarnings("removal")
public class AgentPolicy {
    private static volatile Policy policy;

    private AgentPolicy() {}

    /**
     * Set Agent policy
     * @param policy policy
     */
    public static void setPolicy(Policy policy) {
        if (AgentPolicy.policy == null) {
            AgentPolicy.policy = policy;
        } else {
            throw new SecurityException("The Policy has been set already: " + AgentPolicy.policy);
        }
    }

    /**
     * Check permissions
     * @param permission permission
     */
    public static void checkPermission(Permission permission) {
        final StackWalker walker = StackWalker.getInstance(Option.RETAIN_CLASS_REFERENCE);
        final List<ProtectionDomain> callers = walker.walk(
            frames -> frames.map(StackFrame::getDeclaringClass).map(Class::getProtectionDomain).distinct().collect(Collectors.toList())
        );

        for (final ProtectionDomain domain : callers) {
            if (!policy.implies(domain, permission)) {
                throw new SecurityException("Denied access: " + permission);
            }
        }
    }

    /**
     * Get policy
     * @return policy
     */
    public static Policy getPolicy() {
        return policy;
    }
}
