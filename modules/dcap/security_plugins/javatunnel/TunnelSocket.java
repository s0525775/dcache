/*
 * $Id: TunnelSocket.java,v 1.3 2007-04-05 20:07:11 podstvkv Exp $
 */

package javatunnel;

import java.net.*;
import java.io.*;

public class TunnelSocket extends Socket implements UserBindible {

    private TunnelOutputStream _out = null;
    private TunnelInputStream _in   = null;
    private Convertable _tunnel = null;
    private String _user = "nobody@NOWHERE" ;
    private String _role = null;
    private String _group = null;

    TunnelSocket(Convertable tunnel) throws SocketException {
        super();
        _tunnel = tunnel;
        if ( isConnected()) {
            try{
                _tunnel.auth( this.getRawInputStream(),  this.getRawOutputStream() , this );
            } catch  ( IOException e) {}
        }

    }

    TunnelSocket(SocketImpl impl, Convertable tunnel) throws SocketException {
        super(impl);
        _tunnel = tunnel;
        if ( isConnected()) {
            try{
                _tunnel.auth( this.getRawInputStream(),  this.getRawOutputStream()  , this);
            } catch  ( IOException e) {}
        }

    }

    TunnelSocket(InetAddress address, int port, Convertable tunnel)
    throws IOException {
        super(address, port);
        _tunnel = tunnel;
        if ( isConnected()) {
            try{
                _tunnel.auth( this.getRawInputStream(),  this.getRawOutputStream() , this );
            } catch  ( IOException e) {}
        }
    }


    TunnelSocket(InetAddress address, int port, InetAddress localAddr, int localPort, Convertable tunnel)
    throws IOException {
        super(address, port, localAddr, localPort);
        _tunnel = tunnel;
        if ( isConnected()) {
            try{
                _tunnel.auth( this.getRawInputStream(),  this.getRawOutputStream() , this );
            } catch  ( IOException e) {}
        }

    }

    TunnelSocket(String host, int port, Convertable tunnel)
    throws UnknownHostException, IOException {
        super(host, port);
        _tunnel = tunnel;
        if ( isConnected()) {
            try{
                _tunnel.auth( this.getRawInputStream(),  this.getRawOutputStream() , this );
            } catch  ( IOException e) {}
        }

    }

    TunnelSocket(String host, int port, InetAddress localAddr, int localPort, Convertable tunnel)
    throws IOException {
        super(host, port, localAddr, localPort);
        _tunnel = tunnel;
        if ( isConnected()) {
            try{
                _tunnel.auth( this.getRawInputStream(),  this.getRawOutputStream() , this );
            } catch  ( IOException e) {}
        }

    }

    @Override
    public OutputStream getOutputStream() throws java.io.IOException {

        if(_out == null) {
            _out = new TunnelOutputStream( super.getOutputStream(), _tunnel );
        }

        return _out;

    }

    @Override
    public InputStream getInputStream() throws java.io.IOException {

        if(_in == null) {
            _in = new TunnelInputStream( super.getInputStream() , _tunnel );
        }

        return _in;

    }

    public OutputStream getRawOutputStream() throws java.io.IOException {

        return super.getOutputStream();
    }

    public InputStream getRawInputStream() throws java.io.IOException {

        return super.getInputStream();

    }


    @Override
    public void connect(SocketAddress endpoint) throws IOException {
        this.connect(endpoint, 0);
    }


    @Override
    public void connect(SocketAddress endpoint, int timeout) throws IOException {
        super.connect(endpoint, timeout);

        if( _tunnel != null ) {

            try {
                _tunnel.auth(this.getRawInputStream(),  this.getRawOutputStream() , this);
            } catch( Exception e) {
                e.printStackTrace();
            }

        }
    }


    public void setUserPrincipal(String user) {
        _user = user;
    }


    public String getUserPrincipal() {
        return _user;
    }

	public String getGroup() {

		return _group;
	}

	public String getRole() {

		return _role;
	}

    public void setRole(String newRole) {
        _role = newRole;
    }

    public void setGroup(String newGroup) {
        _group = newGroup;
    }

    public boolean verify() throws IOException {
    	if (_tunnel != null) {
    		if (_tunnel.verify(this.getRawInputStream(), this.getRawOutputStream(), this)) {
    			this.setUserPrincipal(_tunnel.getUserPrincipal());
    			this.setRole (((UserBindible)_tunnel).getRole());
    			this.setGroup(((UserBindible)_tunnel).getGroup());
    			return true;
    		} else
    			return false;
    	}
    	return true;
    }

}