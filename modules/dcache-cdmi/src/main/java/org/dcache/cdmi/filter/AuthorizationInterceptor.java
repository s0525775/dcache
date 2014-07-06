/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

// http://cxf.apache.org/docs/secure-jax-rs-services.html#SecureJAX-RSServices-Authentication
// Has to be CXF 2.7.10 since it needs to be compatible with the newer Jetty-Server and since needs to be compatible
// with JSR311 (version 1.1.1) as well. I can't use JSR311 (version 2.0) since it wouldn't be compatible with the dCache Core.

// More Info: http://cxf.apache.org/docs/jax-rs.html#JAX-RS-FromJAX-RS1.1to2.0
// Quote: "CXF 2.7.10 and CXF 3.0.0 are expected to support existing JAX-RS 1.1 applications."

// If I updated JSR311 to version 2.0, the error message would be (from dCache Core):
// java.lang.RuntimeException: URL [file:/DCACHE/dcache/packages/system-test/target/dcache/share/services/cdmi.batch]: line 10: java.lang.RuntimeException: dmg.util.CommandThrowableException: (3) Failed to create bean 'cdmiService' : javax.ws.rs.BindingPriority
//      at dmg.cells.nucleus.CellShell.execute(CellShell.java:1653) ~[cells-2.10.0-SNAPSHOT.jar:2.10.0-SNAPSHOT]
//      ...

// If I didn't use CXF version 2.7.10, the error message would be (affects cxf-rt-frontend-jaxrs in the main):
// java.lang.NoSuchMethodError: javax.ws.rs.core.Response.getHeaders()Ljavax/ws/rs/core/MultivaluedMap;
//      at org.apache.cxf.jaxrs.utils.JAXRSUtils.setMessageContentType(JAXRSUtils.java:1595) ~[cxf-rt-frontend-jaxrs-2.7.6.jar:2.7.6]
//      ...

package org.dcache.cdmi.filter;

import org.dcache.cdmi.provider.CdmiExceptionMapper;
import static com.google.common.base.Strings.isNullOrEmpty;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import static java.util.Arrays.asList;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.HttpMethod;
import org.apache.cxf.common.security.SimplePrincipal;
import org.apache.cxf.configuration.security.AuthorizationPolicy;
import org.apache.cxf.interceptor.Fault;
import org.apache.cxf.message.Message;
import org.apache.cxf.security.LoginSecurityContext;
import org.apache.cxf.phase.AbstractPhaseInterceptor;
import org.apache.cxf.phase.Phase;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.cdmi.exception.MethodNotAllowedException;
import org.dcache.cdmi.exception.ServerErrorException;
import org.dcache.util.CertificateFactories;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snia.cdmiserver.exception.UnauthorizedException;

/**
 * AuthorizationInterceptor for CDMI door.
 */
public class AuthorizationInterceptor extends AbstractPhaseInterceptor<Message>
{

    private final Logger _log = LoggerFactory.getLogger(AuthorizationInterceptor.class);
    private static final String AUTH_BASIC = "BASIC";
    private static final String X509_CERTIFICATE_ATTRIBUTE = "javax.servlet.request.X509Certificate";

    private final CertificateFactory _cf;
    private boolean _isReadOnly;
    private boolean _isBasicAuthenticationEnabled;
    private LoginStrategy _loginStrategy;
    private FsPath _rootPath = new FsPath();
    private FsPath _uploadPath;

    /**
     * <p>The realm name to use in authentication challenges.</p>
     */
    private String _realm = "dCache";

    public AuthorizationInterceptor()
    {
        super(Phase.PRE_INVOKE);
        _cf = CertificateFactories.newX509CertificateFactory();
    }

    public String getRealm()
    {
        return _realm;
    }

    public void setRealm(String realm)
    {
        _realm = realm;
    }

    public boolean isReadOnly()
    {
        return _isReadOnly;
    }

    /**
     * Specifies whether the door is read only.
     * @param isReadOnly
     */
    public void setReadOnly(boolean isReadOnly)
    {
        _isReadOnly = isReadOnly;
    }

    public void setEnableBasicAuthentication(boolean isEnabled)
    {
        _isBasicAuthenticationEnabled = isEnabled;
    }

    public void setLoginStrategy(LoginStrategy loginStrategy)
    {
        _loginStrategy = loginStrategy;
    }

    public void setRootPath(String path)
    {
        _rootPath = new FsPath(path);
    }

    public String getRootPath()
    {
        return _rootPath.toString();
    }

    public void setUploadPath(String uploadPath)
    {
        this._uploadPath = isNullOrEmpty(uploadPath) ? null : new FsPath(uploadPath);
    }

    public String getUploadPath()
    {
        return Objects.toString(_uploadPath, null);
    }

    @Override
    public void handleMessage(final Message msg) throws Fault
    {
        Subject subject = new Subject();
        HttpServletRequest servletRequest = (HttpServletRequest) msg.get("HTTP.REQUEST");

        if (servletRequest == null) {
            _log.error("HttpServletRequest is null!");
            throw new CdmiExceptionMapper(
                    new ServerErrorException("HttpServletRequest is null!"));
        }

        if (!isAllowedMethod(msg.get(Message.HTTP_REQUEST_METHOD).toString())) {
            _log.debug("Failing {} from {} as door is read-only",
                    msg.get(Message.HTTP_REQUEST_METHOD).toString(), servletRequest.getRemoteAddr());
            throw new CdmiExceptionMapper(
                    new MethodNotAllowedException("Method " + msg.get(Message.HTTP_REQUEST_METHOD).toString() + " is not allowed"));
        }

        try {
            boolean certificate = false;
            boolean origin = false;
            boolean credentials = false;
            certificate = addX509ChainToSubject(servletRequest, subject);
            origin = addOriginToSubject(servletRequest, subject);
            credentials = addPasswordCredentialToSubject(msg, subject);

            if (certificate || credentials || origin) {
                LoginReply login = _loginStrategy.login(subject);
                subject = login.getSubject();

                if (!isAuthorizedMethod(msg.get(Message.HTTP_REQUEST_METHOD).toString(), login)) {
                    throw new PermissionDeniedCacheException("Permission denied: read-only user");
                }

                checkRootPath(msg, login);

                // Add the origin of the request to the subject. This
                // ought to be processed in the LoginStrategy, but our
                // LoginStrategies currently do not process the Origin.
                addOriginToSubject(servletRequest, subject);

                // Process the request as the authenticated user.
                Subject.doAs(subject, new PrivilegedAction() {
                    @Override
                    public Object run() {
                        ContextHolder.set(AccessController.getContext());
                        Subject s1 = Subject.getSubject(AccessController.getContext());
                        msg.put(LoginSecurityContext.class, createSecurityContext(s1));
                        return s1;
                    }
                });

            } else {
                throw new PermissionDeniedCacheException("Permission denied: read-only user");
            }

        } catch (PermissionDeniedCacheException e) {
            _log.warn("{} for path {} and {}", e.getMessage(), msg.get(Message.REQUEST_URI).toString(), subject);
            throw new CdmiExceptionMapper(
                    new UnauthorizedException("Authentication credentials are required", _realm));
        } catch (CacheException e) {
            _log.error("Internal server error: " + e);
            throw new CdmiExceptionMapper(
                    new ServerErrorException(e.getMessage()));
        }
    }

    protected LoginSecurityContext createSecurityContext(final Subject subject)
    {
        return new LoginSecurityContext() {
            @Override
            public Subject getSubject() {
                return subject;
            }

            @Override
            public Set<Principal> getUserRoles() {
                Set<Principal> principals = new HashSet<Principal>();
                principals.add(new Principal() {
                    @Override
                    public String getName() {
                        return "";
                    }
                });
                return principals;
            }

            @Override
            public Principal getUserPrincipal() {
                String principal = null;
                for (Principal prin : subject.getPrincipals()) {
                    if ((prin != null) && (!prin.getName().isEmpty()))
                        principal = prin.getName();
                }
                return new SimplePrincipal(principal);
            }

            @Override
            public boolean isUserInRole(String role) {
                return false;
            }
        };
    }

    private void checkRootPath(Message msg, LoginReply login) throws CacheException
    {
        FsPath userRoot = new FsPath();
        for (LoginAttribute attribute: login.getLoginAttributes()) {
            if (attribute instanceof RootDirectory) {
                userRoot = new FsPath(((RootDirectory) attribute).getRoot());
            }
        }

        String path = msg.get(Message.REQUEST_URI).toString();
        FsPath fullPath = new FsPath(_rootPath, new FsPath(path));
        if (!fullPath.startsWith(userRoot) &&
                (_uploadPath == null || !fullPath.startsWith(_uploadPath))) {
            if (!path.equals("/")) {
                throw new PermissionDeniedCacheException("Permission denied: path outside user's root");
            }
        }
    }

    private boolean isAllowedMethod(String method)
    {
        return !_isReadOnly || isReadMethod(method);
    }

    private boolean isAuthorizedMethod(String method, LoginReply login)
    {
        return !isUserReadOnly(login) || isReadMethod(method);
    }

    private boolean isUserReadOnly(LoginReply login)
    {
        for (LoginAttribute attribute: login.getLoginAttributes()) {
            if (attribute instanceof ReadOnly) {
                return ((ReadOnly) attribute).isReadOnly();
            }
        }
        return false;
    }

    private boolean isReadMethod(String method)
    {
        switch (method) {
            case HttpMethod.GET:
            case HttpMethod.HEAD:
                return true;
            default:
                return false;
        }
    }

    private boolean addX509ChainToSubject(HttpServletRequest request, Subject subject)
            throws CacheException
    {
        boolean result = false;
        Object object = request.getAttribute(X509_CERTIFICATE_ATTRIBUTE);
        if (object instanceof X509Certificate[]) {
            try {
                subject.getPublicCredentials().add(_cf.generateCertPath(asList((X509Certificate[]) object)));
                result = true;
            } catch (CertificateException e) {
                throw new CacheException("Failed to generate X.509 certificate path: " + e.getMessage(), e);
            }
        }
        return result;
    }

    private boolean addOriginToSubject(HttpServletRequest request, Subject subject)
    {
        boolean result = false;
        String address = request.getRemoteAddr();
        try {
            Origin origin = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddress.getByName(address));
            subject.getPrincipals().add(origin);
            result = true;
        } catch (UnknownHostException e) {
            _log.warn("Failed to resolve " + address + ": " + e.getMessage());
        }
        return result;
    }

    private boolean addPasswordCredentialToSubject(Message msg, Subject subject)
    {
        boolean result = false;
        AuthorizationPolicy auth = (AuthorizationPolicy) msg.get(AuthorizationPolicy.class);
        if ((auth != null) && auth.getAuthorizationType().toUpperCase().equals(AUTH_BASIC) && _isBasicAuthenticationEnabled) {
            PasswordCredential credential = new PasswordCredential(auth.getUserName(), auth.getPassword());
            subject.getPrivateCredentials().add(credential);
            result = true;
        }
        return result;
    }

}
