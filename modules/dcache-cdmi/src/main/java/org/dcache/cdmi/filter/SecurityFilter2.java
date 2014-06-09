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
package org.dcache.cdmi.filter;

import static com.google.common.base.Strings.isNullOrEmpty;
import diskCacheV111.util.Base64;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import static java.util.Arrays.asList;
import java.util.List;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.logging.Level;
import javax.security.auth.Subject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.httpclient.HttpStatus;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.util.CertificateFactories;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.snia.cdmiserver.exception.UnauthorizedException;

/*
    <dependency>
      <groupId>javax.ws.rs</groupId>
      <artifactId>javax.ws.rs-api</artifactId>
      <version>2.0</version>
    </dependency>
*/

/**
 * SecurityFilter for CDMI door. Is called. Backup. Throws error message through Maven, conflict between javax.ws.rs-api and jsr311-api.
 * http://comments.gmane.org/gmane.comp.apache.cxf.user/25132
 */

/* When replacing jsr311-api by javax.ws.rs-api, or when adding both jsr311-api and javax.ws.rs-api in POM file:
 * CDMI Server isn't started.
 * java.lang.RuntimeException: URL [file:/DCACHE/dcache/packages/system-test/target/dcache/share/services/cdmi.batch]: line 10: java.lang.RuntimeException: dmg.util.CommandThrowableException: (3) Failed to create bean 'cdmiService' : javax.ws.rs.BindingPriority
 *      at dmg.cells.nucleus.CellShell.execute(CellShell.java:1653) ~[cells-2.10.0-SNAPSHOT.jar:2.10.0-SNAPSHOT]
 *      ....
 */

/* When only leaving jsr311-api in POM file and when not adding javax.ws.rs-api there (as before):
 * CDMI Server is started and Filter is executed till gPlazma.
 * java.lang.NoSuchMethodError: javax.ws.rs.core.Response.getHeaders()Ljavax/ws/rs/core/MultivaluedMap;
 *      at org.apache.cxf.jaxrs.utils.JAXRSUtils.setMessageContentType(JAXRSUtils.java:1595) ~[cxf-rt-frontend-jaxrs-2.7.6.jar:2.7.6]
 *      ....
 *      at org.apache.cxf.phase.PhaseInterceptorChain.doIntercept(PhaseInterceptorChain.java:271) ~[cxf-api-2.7.6.jar:2.7.6]
 *      ....
 *      at org.apache.cxf.transport.http_jetty.JettyHTTPHandler.handle(JettyHTTPHandler.java:72) ~[cxf-rt-transports-http-jetty-2.7.6.jar:2.7.6]
 *      at org.eclipse.jetty.server.handler.ContextHandler.doHandle(ContextHandler.java:1088) ~[jetty-server-8.1.14.v20131031.jar:8.1.14.v20131031]
 *      ....
 */

// Remark: The cxf version 2.7.6 is dependent from the Jetty version.

public class SecurityFilter2 implements ContainerRequestFilter
{

    private final Logger _log = LoggerFactory.getLogger(SecurityFilter2.class);
    private static final String AUTHORIZATION_PROPERTY = "Authorization";
    private static final String X509_CERTIFICATE_ATTRIBUTE =
        "javax.servlet.request.X509Certificate";

    private final CertificateFactory _cf;
    private boolean _isReadOnly;
    private boolean _isBasicAuthenticationEnabled;
    private LoginStrategy _loginStrategy;
    private FsPath _rootPath = new FsPath();
    private FsPath _uploadPath;

    /**
     * <p>The realm name to use in authentication challenges.</p>
     */
    private String _realm = "CDMI Service";

    public SecurityFilter2()
    {
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

    /**
     * <p>The URI information for this request.  This information may be
     * accessed from an <code>Authorizer</code> to make request specific
     * role determinations.</p>
     */
    @Context
    UriInfo uriInfo = null;

    @Context
    private HttpServletRequest servletRequest;

    @Context
    HttpServletResponse response;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        System.out.println("Authentication Process...");

        Subject subject = new Subject();

        if (!isAllowedMethod(requestContext.getMethod())) {
            _log.debug("Failing {} from {} as door is read-only", requestContext.getMethod(), uriInfo.getBaseUri().getHost());
            //manager.getResponseHandler().respondMethodNotAllowed(new EmptyResource(request), response, request);
            requestContext.abortWith(Response
                .status(Response.Status.UNAUTHORIZED)
                .entity("Access denied for this resource.")
                .build());
            return;
        }

        try {
            addX509ChainToSubject(servletRequest, subject);
            addOriginToSubject(servletRequest, subject);
            addPasswordCredentialToSubject(requestContext, subject);

            LoginReply login = _loginStrategy.login(subject);
            subject = login.getSubject();

            if (!isAuthorizedMethod(requestContext.getMethod(), login)) {
                throw new PermissionDeniedCacheException("Permission denied: read-only user");
            }

            checkRootPath(requestContext, login);

            /* Add the origin of the request to the subject. This
             * ought to be processed in the LoginStrategy, but our
             * LoginStrategies currently do not process the Origin.
             */
            addOriginToSubject(servletRequest, subject);

            /* Process the request as the authenticated user.
             */
            Subject.doAs(subject, new PrivilegedAction<Void>() {
                    @Override
                    public Void run()
                        {
                            response.setStatus(HttpStatus.SC_OK);
                            try {
                                servletRequest.authenticate(response);
                            } catch (IOException | ServletException ex) {
                                java.util.logging.Logger.getLogger(SecurityFilter2.class.getName()).log(Level.SEVERE, null, ex);
                            }
                            //filterChain.process(requestContext, response);
                            return null;
                        }
                });
        } catch (CacheException e) {
            _log.error("Internal server error: " + e);
            response.sendError(HttpStatus.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }

    }

    private void checkRootPath(ContainerRequestContext request, LoginReply login) throws CacheException
    {
        FsPath userRoot = new FsPath();
        FsPath userHome = new FsPath();
        for (LoginAttribute attribute: login.getLoginAttributes()) {
            if (attribute instanceof RootDirectory) {
                userRoot = new FsPath(((RootDirectory) attribute).getRoot());
            } else if (attribute instanceof HomeDirectory) {
                userHome = new FsPath(((HomeDirectory) attribute).getHome());
            }
        }

        String path = request.getUriInfo().getAbsolutePath().getPath();
        FsPath fullPath = new FsPath(_rootPath, new FsPath(path));
        if (!fullPath.startsWith(userRoot) &&
                (_uploadPath == null || !fullPath.startsWith(_uploadPath))) {
            if (!path.equals("/")) {
                throw new PermissionDeniedCacheException("Permission denied: path outside user's root");
            }

            try {
                FsPath redirectFullPath = new FsPath(userRoot, userHome);
                String redirectPath = _rootPath.relativize(redirectFullPath).toString();
                URI uri = request.getUriInfo().getAbsolutePath();
                URI redirect = new URI(uri.getScheme(), uri.getAuthority(), redirectPath, null, null);
                try {
                    response.sendRedirect(redirect.toString());
                } catch (IOException ex) {
                    java.util.logging.Logger.getLogger(SecurityFilter2.class.getName()).log(Level.SEVERE, null, ex);
                }
            } catch (URISyntaxException e) {
                throw new CacheException(e.getMessage(), e);
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

    private void addX509ChainToSubject(HttpServletRequest servletRequest, Subject subject)
            throws CacheException
    {
        Object object = servletRequest.getAttribute(X509_CERTIFICATE_ATTRIBUTE);
        if (object instanceof X509Certificate[]) {
            try {
                subject.getPublicCredentials().add(_cf.generateCertPath(asList((X509Certificate[]) object)));
            } catch (CertificateException e) {
                throw new CacheException("Failed to generate X.509 certificate path: " + e.getMessage(), e);
            }
        }
    }

    private void addOriginToSubject(HttpServletRequest servletRequest, Subject subject)
    {
        String address = servletRequest.getRemoteAddr();
        try {
            Origin origin =
                new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddress.getByName(address));
            subject.getPrincipals().add(origin);
        } catch (UnknownHostException e) {
            _log.warn("Failed to resolve " + address + ": " + e.getMessage());
        }
    }

    private void addPasswordCredentialToSubject(ContainerRequestContext request, Subject subject)
    {
        SecurityContext auth = request.getSecurityContext();

        final MultivaluedMap<String, String> headers = request.getHeaders();
        final List<String> authorization = headers.get(AUTHORIZATION_PROPERTY);

        if (auth == null || authorization == null || authorization.isEmpty()) {
            request.abortWith(Response
                .status(Response.Status.UNAUTHORIZED)
                .entity("Access denied for this resource.")
                .build());
            return;
        }

        final String encodedUserPassword = authorization.get(0).replaceFirst(auth.getAuthenticationScheme() + " ", "");

        //Decode username and password
        String usernameAndPassword = Base64.byteArrayToBase64(encodedUserPassword.getBytes());

        //Split username and password tokens
        final StringTokenizer tokenizer = new StringTokenizer(usernameAndPassword, ":");
        final String username = tokenizer.nextToken();
        final String password = tokenizer.nextToken();

        //Verifying Username and password
        System.out.println(username);
        System.out.println(password);

        if (auth.getAuthenticationScheme().equals(SecurityContext.BASIC_AUTH) && _isBasicAuthenticationEnabled) {
            PasswordCredential credential = new PasswordCredential(username, password);
            subject.getPrivateCredentials().add(credential);
        }
    }

}
