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

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;

import java.security.cert.X509Certificate;
import java.util.logging.Level;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.dcache.auth.Subjects;

/**
 * Simple request logging for CDMI door. Interim solution until we
 * switch to a better logging framework with direct support for Jetty.
 */
public class LoggingFilter implements Filter
{
    private final Logger _log = LoggerFactory.getLogger(LoggingFilter.class);

    private FilterConfig config;
    private static final String X509_CERTIFICATE_ATTRIBUTE =
        "javax.servlet.request.X509Certificate";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException
    {
        config = filterConfig;
    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain filterChain)
    {
        HttpServletRequest servletRequest = (HttpServletRequest) request;
        try {
            HttpServletResponse servletResponse = (HttpServletResponse) response;
            ExposingServletResponse seResponse = new ExposingServletResponse(servletResponse);

            filterChain.doFilter(request, response);

            servletRequest = (HttpServletRequest) request;
            int status = seResponse.getStatus();
            if (status != 0) {
                _log.info("{} {} {} {} {}",
                          request.getRemoteAddr(),
                          servletRequest.getMethod(),
                          request.getServletContext().getRealPath(""),
                          getUser(request),
                          status);
            } else {
                _log.info("{} {} {} {}",
                          request.getRemoteAddr(),
                          servletRequest.getMethod(),
                          request.getServletContext().getRealPath(""),
                          getUser(request));
            }
        } catch (RuntimeException e) {
            _log.warn(String.format("%s %s %s %s",
                                    request.getRemoteAddr(),
                                    servletRequest.getMethod(),
                                    request.getServletContext().getRealPath(""),
                                    getUser(request)),
                                    e);
            throw e;
        } catch (IOException | ServletException ex) {
            java.util.logging.Logger.getLogger(LoggingFilter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

     private String getUser(ServletRequest request)
     {
         StringBuilder sb = new StringBuilder();

         String certificateName = getCertificateName(request);
         String subjectName = getSubjectName(request);

         sb.append("[");
         if(certificateName.isEmpty() && subjectName.isEmpty()) {
             sb.append("ANONYMOUS");
         } else {
             sb.append(certificateName);

             if(!certificateName.isEmpty() && !subjectName.isEmpty()) {
                 sb.append("; ");
             }

             sb.append(subjectName);
         }
         sb.append("]");

         return sb.toString();
    }

    private String getCertificateName(ServletRequest request)
    {
        HttpServletRequest servletRequest = (HttpServletRequest) request;

        Object object =
            servletRequest.getAttribute(X509_CERTIFICATE_ATTRIBUTE);

        if (object instanceof X509Certificate[]) {
            X509Certificate[] chain = (X509Certificate[]) object;

            if (chain.length >= 1) {
                return chain[0].getSubjectX500Principal().getName();
            }
        }

        return "";
    }

    private String getSubjectName(ServletRequest request)
    {
        /*
        Auth auth = request.getAuthorization();

        if(auth == null) {
            return "";
        }

        Subject subject = (Subject) auth.getTag();

        if(subject == null) {
            return "";
        }

        if(subject.equals(Subjects.NOBODY)) {
            return "NOBODY";
        }

        if(subject.equals(Subjects.ROOT)) {
            return "ROOT";
        }

        String uid;
        try {
            uid = Long.toString(Subjects.getUid(subject));
        } catch(NoSuchElementException e) {
            uid="unknown";
        }

        List<Long> gids = Longs.asList(Subjects.getGids(subject));

        return "uid=" + uid + ", gid={" + Joiner.on(",").join(gids)+"}";
        */
        return "";
    }

    @Override
    public void destroy()
    {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
