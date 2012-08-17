package dmg.cells.services.login ;

import java.io.* ;


public class ControlBufferedReader extends Reader implements InputHandler {
    private Reader  _reader     = null ;
    private final Object  _lock       = new Object() ;
    private boolean _eof        = false ;
    private String  _onControlC = "" ;
    private final static char  CONTROL_C  =  (char)3 ;
    private final static char  CONTROL_H  =  (char)8 ;
    /**
     * Create a buffering character-input stream that uses a default-sized
     * input buffer.
     *
     * @param  in   A Reader
     */
    public ControlBufferedReader(Reader in) {
       _reader = in ;
    }
    /* (non-Javadoc)
     * @see dmg.cells.services.login.InputHandler#close()
     */
    public void close() throws IOException {
	synchronized (_lock) {
	    if ( _reader == null)return;
	    _reader.close();
	    _reader = null;
	}
    }
    public int read(char cbuf[], int off, int len) throws IOException {
	synchronized (_lock) {
           return _reader.read( cbuf , off , len ) ;
        }
    }
    public void onControlC( String onCC ){
       _onControlC = onCC ;
    }
    /* (non-Javadoc)
     * @see dmg.cells.services.login.InputHandler#readLine()
     */
    public String readLine() throws IOException {
       int n = 0 ;
       synchronized( _lock ){
          if( _eof )return null ;
          StringBuffer s = new StringBuffer(128) ;
          char [] cb = new char[1] ;
          while( true ){
              n = _reader.read( cb , 0 , 1 ) ;
              if( n < 0 ){
                 _eof = true ;
                 return s.length() == 0 ? null : s.toString() ;
              }
//              System.out.println( "-- "+((int)cb[0]));
              switch( cb[0] ){
                 case '\n' :
                 case '\r' :
                    return s.toString() ;
                 case CONTROL_C :
                    return _onControlC ;
                 case CONTROL_H :
                    if( s.length() > 0 )s.setLength(s.length()-1) ;
                 break ;
                 default : s.append( cb[0] ) ;
              }
          }
       }
    }
}