package org.dcache.auth;

import java.security.Principal;
import java.io.Serializable;

public class LoginGidPrincipal implements Principal, Serializable
{
    static final long serialVersionUID = -719644742571312959L;

    private long _gid;

    public LoginGidPrincipal(long gid)
    {
        if (gid < 0) {
            throw new IllegalArgumentException("GID must be non-negative");
        }
        _gid = gid;
    }

    public LoginGidPrincipal(String gid)
    {
        this(Long.parseLong(gid));
    }

    public long getGid()
    {
        return _gid;
    }

    @Override
    public String getName()
    {
        return String.valueOf(_gid);
    }

    @Override
    public String toString()
    {
        return (getClass().getSimpleName() + "[" + getName() + "]");
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof LoginGidPrincipal)) {
            return false;
        }
        LoginGidPrincipal other = (LoginGidPrincipal) obj;
        return (other._gid == _gid);
    }

    @Override
    public int hashCode() {
        return (int) _gid;
    }
}