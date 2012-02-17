// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.2.12.1  2007/03/08 23:09:14  timur
// reduce database usage for cases when monitoring is not enabled
//
// Revision 1.2  2005/03/01 23:10:39  timur
// Modified the database scema to increase database operations performance and to account for reserved space"and to account for reserved space
//
// Revision 1.1  2005/01/14 23:07:15  timur
// moving general srm code in a separate repository
//
// Revision 1.3  2004/11/01 20:41:16  timur
//  fixed the problem causing the exhaust of the jdbc connections
//
// Revision 1.2  2004/08/06 19:35:25  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.2  2004/07/02 20:10:25  timur
// fixed the leak of sql connections, added propogation of srm errors
//
// Revision 1.1.2.1  2004/06/16 19:44:33  timur
// added cvs logging tags and fermi copyright headers at the top, removed Copier.java and CopyJob.java
//

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

/*
 * JobStorage.java
 *
 * Created on April 26, 2004, 3:18 PM
 */

package org.dcache.srm.scheduler;
import java.util.Set;
import java.sql.SQLException;
import java.sql.Connection;
/**
 *
 * @author  timur
 */
public interface JobStorage {
    public Job getJob(Long jobId) throws SQLException;
    public Job getJob(Long jobId, Connection connection) throws SQLException;
    public Set getJobs(String scheduler) throws SQLException;
    public Set getJobs(String scheduler,State state) throws SQLException;
    /**
     * 
     * @param job Job to save
     * @param saveIfMonitoringDisabled if this is false and monitoring jdbc login 
     *         disabled, this operation will be ignored
     * @throws java.sql.SQLException 
     */
    public void saveJob(Job job, boolean saveIfMonitoringDisabled)
    throws SQLException;
}