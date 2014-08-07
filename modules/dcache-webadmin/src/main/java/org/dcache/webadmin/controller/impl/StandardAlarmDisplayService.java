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
package org.dcache.webadmin.controller.impl;

import java.util.Collection;
import java.util.Date;

import org.dcache.alarms.Severity;
import org.dcache.alarms.dao.LogEntry;
import org.dcache.webadmin.controller.IAlarmDisplayService;
import org.dcache.webadmin.controller.util.AlarmTableProvider;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.ILogEntryDAO;
import org.dcache.webadmin.model.util.AlarmJDOUtils;
import org.dcache.webadmin.model.util.AlarmJDOUtils.AlarmDAOFilter;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provider does in-memory filtering and sorts on sortable fields; service
 * delegates values for filtering to provider; provider also holds internal map
 * of the current alarms.
 *
 * @author arossi
 */
public class StandardAlarmDisplayService implements IAlarmDisplayService {

    private static final long serialVersionUID = 6949169602783225125L;

    private final AlarmTableProvider alarmTableProvider = new AlarmTableProvider();
    private final ILogEntryDAO access;

    public StandardAlarmDisplayService(DAOFactory factory) {
        access = checkNotNull(factory.getLogEntryDAO());
    }

    @Override
    public AlarmTableProvider getDataProvider() {
        return alarmTableProvider;
    }


    @Override
    public Collection<String> getPredefinedAlarmTypes() {
        return access.getEntryTypes();
    }

    public boolean isConnected() {
        return access.isConnected();
    }

    /**
     * Calls update, then delete, then refreshes the in-memory list.
     */
    @Override
    public void refresh() {
        if (!isConnected()) {
            return;
        }

        update();
        delete();

        AlarmTableProvider alarmTableProvider = getDataProvider();
        Date after = alarmTableProvider.getAfter();
        Date before = alarmTableProvider.getBefore();
        String severityStr = alarmTableProvider.getSeverity();
        Severity severity = severityStr == null ? null :
            Severity.valueOf(severityStr);
        String type = alarmTableProvider.getType();
        Boolean alarm = alarmTableProvider.isAlarm();
        Integer rangeStart = alarmTableProvider.getFrom();
        Integer rangeEnd = alarmTableProvider.getTo();

        AlarmDAOFilter filter
            = AlarmJDOUtils.getFilter(after, before, severity, type,
                                      alarm, rangeStart, rangeEnd);
        Collection<LogEntry> refreshed = access.get(filter);
        alarmTableProvider.setEntries(refreshed);
    }

    public void shutDown() {
        access.shutDown();
    }

    private void delete() {
        getDataProvider().delete(access);
    }

    private void update() {
        getDataProvider().update(access);
    }
}
