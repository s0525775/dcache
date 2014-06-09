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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Objects;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.util.CertificateFactories;
import static com.google.common.base.Strings.isNullOrEmpty;
import diskCacheV111.util.Base64;
import java.io.IOException;
import static java.util.Arrays.asList;
import java.util.StringTokenizer;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
//import org.snia.cdmiserver.exception.UnauthorizedException;

/**
 * SecurityFilter2 for CDMI door(2). Error: Isn't called/executed from Spring. Old way of Filter implementation.
 */
public class SecurityFilter3 implements Filter
{
    private final Logger _log = LoggerFactory.getLogger(SecurityFilter3.class);
    private static final String AUTHORIZATION_PROPERTY = "Authorization";
    private static final String X509_CERTIFICATE_ATTRIBUTE =
        "javax.servlet.request.X509Certificate";

    private final CertificateFactory _cf;
    private boolean _isReadOnly;
    private boolean _isBasicAuthenticationEnabled;
    private LoginStrategy _loginStrategy;
    private FsPath _rootPath = new FsPath();
    private FsPath _uploadPath;
    private FilterConfig config;

    /**
     * <p>The realm name to use in authentication challenges.</p>
     */
    private String _realm = "CDMI Service";

    @Context
    UriInfo uriInfo = null;

    public SecurityFilter3()
    {
        _cf = CertificateFactories.newX509CertificateFactory();
    }


    public String getRealm()
    {
        return _realm;
    }

    /**
     * Sets the HTTP realm used for basic authentication.
     * @param realm
     */
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
    public void init(FilterConfig filterConfig) throws ServletException
    {
        config = filterConfig;
    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain filterChain)
    {
        System.out.println("Authentication Process...");
        try {
            //Ignore all non-Http requests/responses
            if (!(request instanceof HttpServletRequest && response instanceof HttpServletResponse)) {
                filterChain.doFilter(request, response);
                return;
            }

            Subject subject = new Subject();
            HttpServletRequest servletRequest = (HttpServletRequest) request;

            if (!isAllowedMethod(servletRequest.getMethod())) {
                _log.debug("Failing {} from {} as door is read-only",
                        servletRequest.getMethod(), request.getRemoteAddr());
                //manager.getResponseHandler().respondMethodNotAllowed(new EmptyResource(request), response, request);
                HttpServletResponse resp = (HttpServletResponse) response;
                resp.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
                return;
            }

            addX509ChainToSubject(servletRequest, subject);
            addOriginToSubject(servletRequest, subject);
            addPasswordCredentialToSubject(request, subject, response);

            LoginReply login = _loginStrategy.login(subject);
            subject = login.getSubject();

            if (!isAuthorizedMethod(servletRequest.getMethod(), login)) {
                throw new PermissionDeniedCacheException("Permission denied: read-only user");
            }

            checkRootPath(request, login, response);

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
                            try {
                                filterChain.doFilter(request, response);
                            } catch (IOException | ServletException e) {
                                //manager.getResponseHandler().respondRedirect(response, request, e.getUrl());
                            }
                            return null;
                        }
                });
        } catch (IOException | ServletException e) {
            //manager.getResponseHandler().respondRedirect(response, request, e.getUrl());
            HttpServletResponse resp = (HttpServletResponse) response;
            resp.setStatus(HttpServletResponse.SC_GONE);
        } catch (CacheException e) {
            _log.error("Internal server error: " + e);
            //manager.getResponseHandler().respondServerError(request, response, e.getMessage());
            HttpServletResponse resp = (HttpServletResponse) response;
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    public static String getFullURL(HttpServletRequest request) {
        StringBuffer requestURL = request.getRequestURL();
        String queryString = request.getQueryString();
        if (queryString == null) {
            return requestURL.toString();
        } else {
            return requestURL.append('?').append(queryString).toString();
        }
    }

    /**
     * Verifies that the request is within the root directory of the given user.
     *
     * @throws RedirectException If the request should be redirected
     * @throws PermissionDeniedCacheException If the request is denied
     * @throws CacheException If the request fails
     */
    private void checkRootPath(ServletRequest request, LoginReply login, ServletResponse response) throws CacheException
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

        HttpServletRequest servletRequest = (HttpServletRequest) request;
        String path = request.getServletContext().getRealPath("");  //AbsolutePath
        FsPath fullPath = new FsPath(_rootPath, new FsPath(path));
        if (!fullPath.startsWith(userRoot) &&
                (_uploadPath == null || !fullPath.startsWith(_uploadPath))) {
            if (!path.equals("/")) {
                throw new PermissionDeniedCacheException("Permission denied: path outside user's root");
            }

            try {
                FsPath redirectFullPath = new FsPath(userRoot, userHome);
                String redirectPath = _rootPath.relativize(redirectFullPath).toString();
                URI uri = new URI(getFullURL(servletRequest));  //AbsoluteURI
                URI redirect = new URI(uri.getScheme(), uri.getAuthority(), redirectPath, null, null);
                //throw new RedirectException(null, redirect.toString());
                HttpServletResponse resp = (HttpServletResponse) response;
                resp.setStatus(HttpServletResponse.SC_GONE);
            } catch (URISyntaxException e) {
                throw new CacheException(e.getMessage(), e);
            }
        }
    }

    private void addX509ChainToSubject(HttpServletRequest request, Subject subject)
            throws CacheException
    {
        Object object = request.getAttribute(X509_CERTIFICATE_ATTRIBUTE);
        if (object instanceof X509Certificate[]) {
            try {
                subject.getPublicCredentials().add(_cf.generateCertPath(asList((X509Certificate[]) object)));
            } catch (CertificateException e) {
                throw new CacheException("Failed to generate X.509 certificate path: " + e.getMessage(), e);
            }
        }
    }

    private void addOriginToSubject(HttpServletRequest request, Subject subject)
    {
        String address = request.getRemoteAddr();
        try {
            Origin origin = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddress.getByName(address));
            subject.getPrincipals().add(origin);
        } catch (UnknownHostException e) {
            _log.warn("Failed to resolve " + address + ": " + e.getMessage());
        }
    }

    private void addPasswordCredentialToSubject(ServletRequest request, Subject subject, ServletResponse response)
    {
        HttpServletRequest servletRequest = (HttpServletRequest) request;
        final String authorization = servletRequest.getHeader(AUTHORIZATION_PROPERTY);

        if (!(isNullOrEmpty(authorization) && (authorization.length() > 5) && authorization.substring(0,5).equalsIgnoreCase("BASIC") && _isBasicAuthenticationEnabled)) {
            HttpServletResponse resp = (HttpServletResponse) response;
            resp.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            return;
        }

        //Decode username and password
        String usernameAndPassword = Base64.byteArrayToBase64(authorization.substring(5).getBytes());

        //Split username and password tokens
        final StringTokenizer tokenizer = new StringTokenizer(usernameAndPassword, ":");
        final String username = tokenizer.nextToken();
        final String password = tokenizer.nextToken();

        //Verifying Username and password
        System.out.println(username);
        System.out.println(password);

        PasswordCredential credential = new PasswordCredential(username, password);
        subject.getPrivateCredentials().add(credential);
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

    @Override
    public void destroy()
    {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
