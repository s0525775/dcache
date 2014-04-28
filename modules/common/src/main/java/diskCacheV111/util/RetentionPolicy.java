// $Id: RetentionPolicy.java,v 1.4 2007-08-03 15:46:04 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.3  2007/01/12 21:07:51  timur
// made correction for the CUSTODIAL space selection
//
// Revision 1.2  2007/01/10 23:03:02  timur
// more consistent argument names
//
// Revision 1.1  2006/09/25 21:48:07  timur
// move AccessLatency and RetentionPolicy classes to diskCacheV111.util
//
// Revision 1.5  2006/09/21 08:48:52  tigran
// added equals method
//
// Revision 1.4  2006/09/19 00:37:48  timur
// more work for space reservation
//
// Revision 1.3  2006/09/15 22:37:44  timur
// started on storing the RetentionPolicy/AccessLatency in Pnfs
//
// Revision 1.2  2006/08/15 22:09:45  timur
// got the messages to get through to space manager
//
// Revision 1.1  2006/07/19 05:09:09  timur
// adding more Space Manager code
//
// Revision 1.1  2006/07/16 05:48:57  timur
// new explicit space manger
//
// Revision 1.5  2006/04/26 17:17:56  timur
// store the history of the state transitions in the database
//
// Revision 1.4  2006/04/21 22:58:28  timur
// we do not need a thread running when we start a remote transfer, but we want to control the number of the transfers, I hope the code accomplishes this now, though an addition of the new job state
//
// Revision 1.3  2005/05/04 21:54:52  timur
// new scheduling policy on restart for put and get request - do not schedule the request if the user does not update its status
//
// Revision 1.2  2005/03/30 22:42:11  timur
// more database schema changes
//
// Revision 1.1  2005/01/14 23:07:15  timur
// moving general srm code in a separate repository
//
// Revision 1.5  2004/11/17 21:56:49  timur
// adding the option which allows to store the pending or running requests in memory, fixed a restore from database bug
//
// Revision 1.4  2004/11/09 08:04:48  tigran
// added SerialVersion ID
//
// Revision 1.3  2004/08/06 19:35:25  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.2.2.2  2004/06/18 22:20:53  timur
// adding sql database storage for requests
//
// Revision 1.2.2.1  2004/06/16 19:44:34  timur
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
 * FileState.java
 *
 * Created on March 19, 2004, 2:51 PM
 */

package diskCacheV111.util;

import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 *
 * @author  timur
 */
public final class RetentionPolicy implements Serializable {
    private static final long serialVersionUID = -2206085171393244383L;
    private final String _name;
    private final int _id;

    public static final RetentionPolicy REPLICA        = new RetentionPolicy("REPLICA",    2);
    public static final RetentionPolicy OUTPUT     = new RetentionPolicy("OUTPUT", 1);
    public static final RetentionPolicy CUSTODIAL     = new RetentionPolicy("CUSTODIAL", 0);

    /**
     * Creates a new instance of FileState
     */
    private RetentionPolicy(String name,int id) {
        _name = name;
        _id = id;
    }

    public static RetentionPolicy[] getAllPoliciess() {
	    return getAllPolicies();
    }

    public static RetentionPolicy[] getAllPolicies() {
	    return new RetentionPolicy[] {
		    REPLICA,
		    OUTPUT,
		    CUSTODIAL};
    }
    public String toString() {
        return _name;
    }

    public int getId() {
        return _id;
    }
    /**
     * this package visible method is used to restore the FileState from
     * the database
     */
    public static RetentionPolicy getRetentionPolicy(String policy) throws IllegalArgumentException {
        if(policy == null || policy.equalsIgnoreCase("null")) {
            throw new NullPointerException(" null state ");
        }

        if(REPLICA._name.equalsIgnoreCase(policy)) {
            return REPLICA;
        }

        if(OUTPUT._name.equalsIgnoreCase(policy)) {
            return OUTPUT;
        }

        if(CUSTODIAL._name.equalsIgnoreCase(policy)) {
            return CUSTODIAL;
        }
        try{
            int id = Integer.parseInt(policy);
            return getRetentionPolicy(id);
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Unknown Policy");
        }
    }

    public static RetentionPolicy getRetentionPolicy(int id) throws IllegalArgumentException {

        if(REPLICA._id == id) {
            return REPLICA;
        }

        if(OUTPUT._id == id) {
            return OUTPUT;
        }

        if(CUSTODIAL._id == id) {
            return CUSTODIAL;
        }

        throw new IllegalArgumentException("Unknown policy Id");
    }

    public static RetentionPolicy valueOf(String value) {
        return getRetentionPolicy(value);
    }

    public boolean equals(Object obj) {
        return ( obj instanceof RetentionPolicy) && ( ((RetentionPolicy)obj).getId() == this.getId() );
    }

    public int hashCode() {
        return _name.hashCode();
    }

    public Object readResolve() throws ObjectStreamException {
       return RetentionPolicy.getRetentionPolicy(getId());
    }


}
