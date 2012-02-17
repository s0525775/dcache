/*
 * DelegationTestClient.java
 *
 * Created on December 17, 2004, 3:03 PM
 */

package org.dcache.srm.security;
import java.util.Hashtable;
import electric.net.socket.ISocketFactory;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketImpl;
import java.net.SocketAddress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.DataOutputStream;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.security.GeneralSecurityException;


//import org.globus.net.GSIServerSocketFactory;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GlobusCredentialException;
import org.globus.gsi.TrustedCertificates;

import org.globus.gsi.gssapi.GlobusGSSManagerImpl;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;

import org.globus.gsi.gssapi.net.GssSocket;
import org.globus.gsi.gssapi.net.GssSocketFactory;
import org.globus.gsi.gssapi.net.impl.GSIGssSocket;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.GlobusCredentialException;

import org.gridforum.jgss.ExtendedGSSContext;
import org.gridforum.jgss.ExtendedGSSManager;
import org.gridforum.jgss.ExtendedGSSCredential;

import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.GSSConstants;


import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
/**
 *
 * @author  timur
 */
public class DelegationTestClient {

    public void say(String s){
        System.out.println(s);
        
    }
    public void esay(String s){
        System.err.println(s);
        
    }
    public void esay(Throwable t){
        t.printStackTrace();
        
    }
    
    public static GSSCredential createUserCredential(
    String x509UserProxy)  throws GlobusCredentialException, GSSException {
        if(x509UserProxy != null) {
            GlobusCredential gcred = new GlobusCredential(x509UserProxy);
            GSSCredential cred =
            new GlobusGSSCredentialImpl(gcred, GSSCredential.INITIATE_ONLY);
            return cred;
        }
        GlobusCredential gcred = GlobusCredential.getDefaultCredential();
        GSSCredential cred = new GlobusGSSCredentialImpl(gcred, GSSCredential.INITIATE_ONLY);
        return  cred;
        
    }

    public java.net.Socket createSocket(java.net.InetAddress inetAddress,
    int port, String x509UserProxy)
    throws IOException {
        say("createSocket("+inetAddress+","+port+")");
        Socket s =null;
        try {
            GSSCredential credential = createUserCredential(x509UserProxy);
            if(credential == null) {
                esay("createSocket: credential is null !!!");
                throw new IOException("credential is null !!!");
            }
            say("createSocket() user credential is "+credential.getName());
            GSSManager manager = ExtendedGSSManager.getInstance();
            org.globus.gsi.gssapi.auth.GSSAuthorization gssAuth = 
            org.globus.gsi.gssapi.auth.HostAuthorization.getInstance();
            GSSName targetName = gssAuth.getExpectedName(null, inetAddress.getCanonicalHostName());
            ExtendedGSSContext context =
            (ExtendedGSSContext) manager.createContext(targetName,
                GSSConstants.MECH_OID,
                credential,
                GSSContext.DEFAULT_LIFETIME);
            context.setOption(GSSConstants.GSS_MODE,
            GSIConstants.MODE_GSI);
            context.requestCredDeleg(true);
             context.setOption(GSSConstants.DELEGATION_TYPE, GSIConstants.DELEGATION_TYPE_FULL);
            //        context.setOption(GSSConstants.DELEGATION_TYPE, GSIConstants.DELEGATION_TYPE_LIMITED);
            s = new Socket(inetAddress,port);
            //say("createSocket() created pain socket "+s);
            GSIGssSocket gsiSocket = new GSIGssSocket(s, context);
            //say("createSocket() gsiSocket "+gsiSocket);
            gsiSocket.setUseClientMode(true);
            gsiSocket.setAuthorization(gssAuth);
            //say("createSocket() setting wrap mode to SSL_MODE");
            gsiSocket.setWrapMode(GssSocket.SSL_MODE);
            //say("createSocket() staring handshake");
            gsiSocket.startHandshake();
            //say("createSocket() handshake complete");
            s = gsiSocket;
        }
        catch(Exception e) {
            //esay("createSocket() failed with exception:");
            //esay(e);
            if(s!=null) {
                try {
                    s.close();
                }
                catch(Exception e1) {
                }
            }
            // I now think we should not unlock socket here, since the 
            // toolkit that uses the factory tries more than one time to create socket
            // without propogation of the exception
            // we just need to make sure that the code, which uses this sockets (SRMClientV1)
            // handles errors correctly
            //saveSocketAndUnlock(null);
            //say("createSocket() exception: "+e);
            throw new IOException(e.toString());
        }
        //say("createSocket() returning socket "+s);
        return s;
        
    }
    private String middleServerHost ;
    private int middleServerPort;
    private String destServerHost;
    private int destServerPort;
    private String proxy;

    /** Creates a new instance of DelegationTestClient */
    public DelegationTestClient(            String middleServerHost ,
    int middleServerPort,
    String destServerHost,
    int destServerPort,
    String proxy) {
    this.middleServerHost =middleServerHost;
    this.middleServerPort = middleServerPort;
    this.destServerHost = destServerHost;
    this.destServerPort = destServerPort;
    this.proxy=proxy;

    }
    
    public void delegate() throws IOException {
        Socket s = createSocket(InetAddress.getByName(middleServerHost),middleServerPort, proxy);
        say("connected to "+s);
        DataOutputStream outStream = 
		new DataOutputStream(s.getOutputStream());
        
        outStream.writeUTF(destServerHost+" "+destServerPort);
        say("wrote "+destServerHost+" "+destServerPort);
        outStream.close();
        
    }
    
    public static final void main(String args[]) throws IOException{
        String middleServerHost = args[0];
        int middleServerPort = Integer.parseInt(args[1]);
        String destServerHost = args[2];
        int destServerPort = Integer.parseInt(args[3]);
        String proxy = args.length> 4? args[4]:(String)null;
        DelegationTestClient client = new  DelegationTestClient( middleServerHost ,
         middleServerPort,
         destServerHost,
         destServerPort,
        proxy);
        client.delegate();
    }
}