package dmg.util.edb ;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
public class JdbmBasic implements JdbmSerializable {

    private static final long serialVersionUID = -4068127117713991402L;
    private String _string = "" ;
    private JdbmBasic _root;
    public JdbmBasic(){}
    public JdbmBasic( String s ){
       int n = s.indexOf(":") ;
       if( n < 0 ){
           _root = null ;
           _string = s ;
       }else{
           _root = new JdbmBasic( s.substring(n+1) ) ;
           _string = s.substring(0,n) ;
       }
    }
    @Override
    public void writeObject( ObjectOutput out )
           throws IOException {

       out.writeUTF( _string ) ;
       if( _root == null ){
          out.writeInt(0) ;
       }else{
          out.writeInt(1) ;
          out.writeObject( _root ) ;
       }
    }
    @Override
    public void readObject( ObjectInput in )
           throws IOException, ClassNotFoundException {

       _string = in.readUTF() ;
       int flag = in.readInt() ;
       System.out.println( "Got="+_string+":"+flag) ;
       if( flag ==  0 ) {
           _root = null;
       } else {
           _root = (JdbmBasic) in.readObject();
       }
    }
    @Override
    public int getPersistentSize() { return 0 ; }
    public String toString(){
       if( _root == null ) {
           return _string;
       } else {
           return _string + ":" + _root.toString();
       }
    }

    public static void main( String [] args )throws Exception {
        if( args.length == 0 ){
            JdbmBasic jdbm = new JdbmBasic("Otto") ;
            JdbmObjectOutputStream out =
               new JdbmObjectOutputStream(
                   new DataOutputStream(
                       new FileOutputStream( "xxx" ) ) ) ;
            out.writeObject( new JdbmBasic( "otto:karl:waste" ) ) ;
            out.close() ;
        }else {
            JdbmObjectInputStream in =
              new JdbmObjectInputStream(
                  new DataInputStream(
                      new FileInputStream("xxx") ) ) ;
            JdbmBasic jdbm;
            while( true ){
               try{
                 jdbm = (JdbmBasic) in.readObject() ;
                 System.out.println( jdbm.toString() ) ;
               }catch(IOException ee ){
                 break ;
               }
            }
            in.close() ;
        }
    }
}
