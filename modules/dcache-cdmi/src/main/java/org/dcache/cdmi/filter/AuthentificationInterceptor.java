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
// Has to be CXF 2.7.10 partially since it needs to be compatible with the newer Jetty-Server and since needs to be compatible
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

import static com.google.common.base.Strings.isNullOrEmpty;
import diskCacheV111.util.Base64;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.Principal;
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
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.cxf.configuration.security.AuthorizationPolicy;
import org.apache.cxf.interceptor.Fault;
import org.apache.cxf.jaxrs.ext.RequestHandler;
import org.apache.cxf.jaxrs.model.ClassResourceInfo;
import org.apache.cxf.message.Message;
import org.apache.cxf.security.SecurityContext;
import org.apache.cxf.service.Service;
import org.apache.cxf.service.model.BindingOperationInfo;
import org.apache.cxf.interceptor.security.AccessDeniedException;
import org.apache.cxf.jaxrs.utils.HttpUtils;
import org.apache.cxf.phase.AbstractPhaseInterceptor;
import org.apache.cxf.phase.Phase;
import org.apache.cxf.service.invoker.MethodDispatcher;
import org.apache.ws.security.WSSecurityEngineResult;
import org.apache.ws.security.handler.WSHandlerConstants;
import org.apache.ws.security.handler.WSHandlerResult;
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

/**
 * AuthentificationInterceptor3 for CDMI door(3). Error: Isn't called/executed from Spring. Jersey implementation (as SNIA does).
 */
public class AuthentificationInterceptor extends AbstractPhaseInterceptor<Message>
{

    private final Logger _log = LoggerFactory.getLogger(AuthentificationInterceptor.class);
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

    public AuthentificationInterceptor()
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
    public void handleMessage(Message msg) throws Fault
    {
        System.out.println("Authentication Process...");

        for (Object key : msg.keySet()) {
            System.out.println("KeySet: " + key.toString());
        }

        AuthorizationPolicy policy = (AuthorizationPolicy) msg.get(AuthorizationPolicy.class);
        if (policy == null) {
            //throw new AccessDeniedException("Unauthorized");
        }

        SecurityContext securityContext = (SecurityContext) msg.get(SecurityContext.class);
        if (securityContext == null) {
            //throw new AccessDeniedException("Unauthorized");
        }

        if (policy != null) {
            String username = policy.getUserName();
            String password = policy.getPassword();
            System.out.println("Username: " + username);
            System.out.println("Password: " + password);
            System.out.println("AuthType: " + policy.getAuthorizationType());
        }

        if (securityContext != null) {
            System.out.println("SC: " + securityContext.toString());
        }

        Method method = getTargetMethod(msg);

        String endpointAddress = HttpUtils.getEndpointAddress(msg);
        Object basePathProperty = msg.get(Message.BASE_PATH);
        Object requestMethod = msg.get(Message.HTTP_REQUEST_METHOD);
        Object requestURL = msg.get(Message.REQUEST_URL);
        Object requestURI = msg.get(Message.REQUEST_URI);
        Object pathInfo = msg.get(Message.PATH_INFO);

        System.out.println("Method: " + getTargetMethod(msg).getName());
        System.out.println("Endpoint: " + endpointAddress);
        System.out.println("BasePath: " + basePathProperty.toString());
        System.out.println("requestMethod: " + requestMethod.toString());
        System.out.println("requestURL: " + requestURL.toString());
        System.out.println("requestURI: " + requestURI.toString());
        System.out.println("pathInfo: " + pathInfo.toString());

        HttpServletRequest request = (HttpServletRequest) msg.get("HTTP.REQUEST");
        if (request != null) {
            X509Certificate[] certs =  (X509Certificate[]) request.getAttribute(X509_CERTIFICATE_ATTRIBUTE);
            if (certs != null) {
                System.out.println("Certs: " + certs.length);
            }
        }

        if (authorize(securityContext, method)) {
            return;
        }

        throw new AccessDeniedException("Unauthorized");

        /*
        Subject subject = new Subject();

        if (!isAllowedMethod(requestMethod.toString())) {
            _log.debug("Failing {} from {} as door is read-only", requestMethod.toString(), endpointAddress);
            //manager.getResponseHandler().respondMethodNotAllowed(new EmptyResource(request), response, request);
            Response.status(HttpStatus.SC_METHOD_NOT_ALLOWED)
                .entity("Access denied for this resource.")
                .header("WWW-Authenticate", "Basic")
                .build();
            return null;
        }

        try {
            addX509ChainToSubject(servletRequest, subject);
            addOriginToSubject(servletRequest, subject);
            addPasswordCredentialToSubject(request, subject);

            LoginReply login = _loginStrategy.login(subject);
            subject = login.getSubject();

            if (!isAuthorizedMethod(request.getMethod(), login)) {
                throw new PermissionDeniedCacheException("Permission denied: read-only user");
            }

            checkRootPath(request, login);

            // Add the origin of the request to the subject. This
            // ought to be processed in the LoginStrategy, but our
            // LoginStrategies currently do not process the Origin.
            addOriginToSubject(servletRequest, subject);

            // Process the request as the authenticated user.
            Subject.doAs(subject, new PrivilegedAction<Void>() {
                    @Override
                    public Void run()
                        {
                            try {
                                servletResponse.setStatus(HttpStatus.SC_OK);
                                servletRequest.authenticate(servletResponse);
                                //filterChain.process(requestContext, response);
                            } catch (IOException | ServletException ex) {
                                java.util.logging.Logger.getLogger(AuthentificationInterceptor.class.getName()).log(Level.SEVERE, null, ex);
                            }
                            return null;
                        }
                });
        } catch (CacheException e) {
            try {
                _log.error("Internal server error: " + e);
                servletResponse.sendError(HttpStatus.SC_INTERNAL_SERVER_ERROR, e.getMessage());
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(AuthentificationInterceptor.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        */
    }

    protected Method getTargetMethod(Message msg) {
        BindingOperationInfo bop = msg.getExchange().get(BindingOperationInfo.class);
        if (bop != null) {
            MethodDispatcher md = (MethodDispatcher)
                msg.getExchange().get(Service.class).get(MethodDispatcher.class.getName());
            return md.getMethod(bop);
        }
        Method method = (Method) msg.get("org.apache.cxf.resource.method");
        if (method != null) {
            return method;
        }
        throw new AccessDeniedException("Method is not available : Unauthorized");
    }

    protected boolean authorize(SecurityContext sc, Method method) {
        if (sc != null) {
            System.out.println("Authorize: " + sc.getUserPrincipal().getName());
            if (sc.getUserPrincipal() != null) {
                _log.trace(sc.getUserPrincipal().getName() + " is not authorized");
            }
        }
        return false;
    }

    /*
    private void checkRootPath(ContainerRequest request, LoginReply login) throws CacheException
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

        String path = uriInfo.getAbsolutePath().getPath();
        FsPath fullPath = new FsPath(_rootPath, new FsPath(path));
        if (!fullPath.startsWith(userRoot) &&
                (_uploadPath == null || !fullPath.startsWith(_uploadPath))) {
            if (!path.equals("/")) {
                throw new PermissionDeniedCacheException("Permission denied: path outside user's root");
            }

            try {
                FsPath redirectFullPath = new FsPath(userRoot, userHome);
                String redirectPath = _rootPath.relativize(redirectFullPath).toString();
                URI uri = uriInfo.getAbsolutePath();
                URI redirect = new URI(uri.getScheme(), uri.getAuthority(), redirectPath, null, null);
                try {
                    servletResponse.sendRedirect(redirect.toString());
                } catch (IOException ex) {
                    java.util.logging.Logger.getLogger(AuthentificationInterceptor.class.getName()).log(Level.SEVERE, null, ex);
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

    private void addPasswordCredentialToSubject(ContainerRequest request, Subject subject)
    {
        final String authorization = servletRequest.getHeader(AUTHORIZATION_PROPERTY);
        System.out.println("Auth: " + authorization);

        if (isNullOrEmpty(authorization)) {
            try {
                _log.debug("Failing {} from {} as door is read-only", request.getMethod(), uriInfo.getBaseUri().getHost());
                //manager.getResponseHandler().respondMethodNotAllowed(new EmptyResource(request), response, request);
                servletResponse.sendError(HttpStatus.SC_UNAUTHORIZED, "Access denied for this resource.");
                //request.abortWith(Response
                //    .status(Response.Status.UNAUTHORIZED)
                //    .entity("Access denied for this resource.")
                //    .build());
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(AuthentificationInterceptor.class.getName()).log(Level.SEVERE, null, ex);
            }
            return;
        }

        final String encodedUserPassword = authorization.replaceFirst(request.getAuthenticationScheme() + " ", "");

        //Decode username and password
        String usernameAndPassword = Base64.byteArrayToBase64(encodedUserPassword.getBytes());

        //Split username and password tokens
        final StringTokenizer tokenizer = new StringTokenizer(usernameAndPassword, ":");
        final String username = tokenizer.nextToken();
        final String password = tokenizer.nextToken();

        //Verifying Username and password
        System.out.println(username);
        System.out.println(password);

        if (request.getAuthenticationScheme().equals(SecurityContext.BASIC_AUTH) && _isBasicAuthenticationEnabled) {
            PasswordCredential credential = new PasswordCredential(username, password);
            subject.getPrivateCredentials().add(credential);
        }
    }
    */

}
