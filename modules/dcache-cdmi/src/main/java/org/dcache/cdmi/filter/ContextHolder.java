package org.dcache.cdmi.filter;

import java.security.AccessControlContext;

// http://stackoverflow.com/questions/4318780/retrieving-the-subject-executing-a-java-security-privilegedaction-at-runtime

public class ContextHolder {

    private static ThreadLocal<AccessControlContext> accCtx = new ThreadLocal<AccessControlContext>();

    public static void set(AccessControlContext acc) {
        accCtx.set(acc);
    }

    public static AccessControlContext get() {
        return accCtx.get();
    }

}
