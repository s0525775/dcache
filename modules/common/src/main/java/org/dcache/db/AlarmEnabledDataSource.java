/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.db;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.Severity;
import org.dcache.util.NetworkUtils;

/**
 * A decorator which allows exception thrown by the connection
 * factory to be marked as alarms.
 *
 * @author arossi
 */
public class AlarmEnabledDataSource implements DataSource, Closeable {

    private static final org.slf4j.Logger LOGGER =
                    LoggerFactory.getLogger(DataSource.class);
    private static final String DB_CONNECTION_FAILURE = "DB_CONNECTION_FAILURE";

    private final DataSource delegate;
    private final String connectorName;
    private final String url;

    /**
     * @param serviceName alarms will be identified by a combination of
     *        this name, the connection url and the canonical name of
     *        this client host.
     */
    public AlarmEnabledDataSource(String url,
                                  String connectorName,
                                  DataSource delegate) {
        this.connectorName = connectorName;
        this.url = url;
        this.delegate = delegate;
    }

    public PrintWriter getLogWriter() throws SQLException {
        return delegate.getLogWriter();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return delegate.unwrap(iface);
    }

    public void setLogWriter(PrintWriter out) throws SQLException {
        delegate.setLogWriter(out);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return delegate.isWrapperFor(iface);
    }

    public Connection getConnection() throws SQLException {
        try {
            return delegate.getConnection();
        } catch (SQLException sql) {
            LOGGER.error(AlarmMarkerFactory.getMarker(Severity.CRITICAL,
                                                      DB_CONNECTION_FAILURE,
                                                      url,
                                                      connectorName,
                                                      NetworkUtils.getCanonicalhostname()),
                         "Could not get connection to database", sql);
            throw sql;
        }
    }

    public void setLoginTimeout(int seconds) throws SQLException {
        delegate.setLoginTimeout(seconds);
    }

    public Connection getConnection(String username, String password)
                    throws SQLException {

        try {
            return delegate.getConnection(username, password);
        } catch (SQLException sql) {
            LOGGER.error(AlarmMarkerFactory.getMarker(Severity.CRITICAL,
                                                      DB_CONNECTION_FAILURE,
                                                      url,
                                                      connectorName,
                                                      NetworkUtils.getCanonicalhostname()),
                         "Could not get connection to database", sql);
            throw sql;
        }
    }

    public int getLoginTimeout() throws SQLException {
        return delegate.getLoginTimeout();
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return delegate.getParentLogger();
    }

    public DataSource getDelegate() {
        return delegate;
    }

    public void close() throws IOException {
        if (delegate instanceof HikariDataSource) {
            ((HikariDataSource) delegate).shutdown();
        } else if (delegate instanceof Closeable) {
            ((Closeable) delegate).close();
        }
    }

    public void shutdown() {
        try {
            close();
        } catch (IOException t) {
            LOGGER.warn("{}: shutdown encountered a problem: {}",
                            connectorName,
                            t.getMessage() );
        }
    }
}
