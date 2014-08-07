package diskCacheV111.srm.dcache;


import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import diskCacheV111.vehicles.Message;

import org.dcache.auth.FQAN;

import static com.google.common.base.Preconditions.*;



/**
 * Query an SRM instance for a delegated credential that matches the given DN
 * and primary FQAN.
 */
public class SrmRequestCredentialMessage extends Message
{
    private static final long serialVersionUID = 1L;

    private final FQAN _primaryFqan;
    private final String _dn;

    /* The credential */
    private PrivateKey _privateKey;
    private X509Certificate[] _certificates;

    /**
     * Create a message to query SRM for a credential.  The DN must be
     * specified but the primaryFQAN may be omitted by specifying null.
     */
    public SrmRequestCredentialMessage(String dn, @Nullable String primaryFqan)
    {
        _dn = checkNotNull(dn);
        _primaryFqan = primaryFqan == null ? null : new FQAN(primaryFqan);
    }

    @Nullable
    public FQAN getPrimaryFqan()
    {
        return _primaryFqan;
    }

    @Nonnull
    public String getDn()
    {
        return _dn;
    }

    public void setPrivateKey(PrivateKey key)
    {
        _privateKey = checkNotNull(key);
    }

    @Nonnull
    public PrivateKey getPrivateKey()
    {
        checkState(hasCredential(), "Message has no credential");
        return _privateKey;
    }

    public void setCertificateChain(X509Certificate[] certificates)
    {
        checkNotNull(certificates);
        checkArgument(certificates.length != 0,
                "credential is invalid if certificate list is empty.");
        _certificates = certificates;
    }

    @Nonnull
    public X509Certificate[] getCertificateChain()
    {
        checkState(hasCredential(), "Message has no credential");
        return _certificates;
    }

    public boolean hasCredential()
    {
        return _privateKey != null && _certificates != null;
    }
}
