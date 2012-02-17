package dmg.util.edb ;

import java.io.* ;

public class JdbmFileHeader implements JdbmSerializable {
    private final static int MAGIC = 0x13579acf ;
    int   _magic                  = 0x13579acf ;
    int   _blockSize              = 0 ;
    int   _elementsPerBucket      = 0 ;
    long  _nextUnallocatedAddress = 0 ;
    
    //
    // the expandable hash directory
    // (is not a class because we need all the space
    //  of the block for 2**n elements )
    //
    long          _directoryAddress = 0 ;
    long   []     _directory        = null ;
    int           _directorySize    = 0 ;
    int           _directoryBits    = 0 ;
    boolean       _directoryChanged = false ;
    //
    //
    long              _avListAddress = 0 ;
    JdbmAvElementList _avList = null ;
    
    public JdbmFileHeader(){}
    public JdbmFileHeader( int blockSize ){
       _blockSize = blockSize ;
       
       initDirectory( _blockSize ) ;
    }
    //
    // directory manupulation
    //
    private void initDirectory( int maxBytes ){
      int bits  ;
      int size  ;
      for( bits = 1 , size = 2 ; size < maxBytes ; bits++ , size *= 2 ) ;
      if( size != maxBytes )
        throw new
        IllegalArgumentException( "block size not 2**n" ) ;
        
      _directorySize = size ;
      _directoryBits = bits ;
      _directoryChanged = true ;
      _directory = new long[_directorySize] ;
      
    }
    public void expandDirectory(){
       _directorySize *= 2 ;
       long [] newAddr = new long[_directorySize] ;
       int n = 0 ;
       for( int i = 0 ; i < _directorySize ; i+= 2 , n++ )
           newAddr[i] = newAddr[i+1] = _directory[n] ;
       _directoryBits ++ ; 
       _directory = newAddr ;
       _directoryChanged = true ;
    }
    public long [] getDirectory(){ return _directory ; }
    public int getDirectorySize(){ return _directorySize * 8 ; }
    public long   getDirectoryAddress(){ return _directoryAddress ; }
    public String toString(){
       StringBuffer sb = new StringBuffer() ;
       sb.append( "dir{b=" ).append(_directoryBits).
          append(";e=").append(_directorySize).
          append(";s=").append(getDirectorySize()).
          append("}") ;
       return sb.toString() ;
    }
    public void writeObject( ObjectOutput out )
           throws java.io.IOException {
       out.writeInt( _magic ) ;
       out.writeInt( _blockSize ) ;
       out.writeInt( _elementsPerBucket ) ;
       out.writeLong( _nextUnallocatedAddress ) ;
       out.writeLong( _directoryAddress ) ;
       out.writeInt( _directorySize ) ;
       out.writeInt( _directoryBits ) ;
       out.writeLong( _avListAddress ) ;
       out.writeInt(_magic );
       return ;   
    }
    public void readObject( ObjectInput in )
           throws java.io.IOException, ClassNotFoundException {
       _magic                  = in.readInt() ;
       if( _magic != MAGIC )
          throw new 
          IOException( "Not a JDBM file" ) ;
          
       _blockSize              = in.readInt() ;
       _elementsPerBucket      = in.readInt() ;
       _nextUnallocatedAddress = in.readLong() ;
       _directoryAddress       = in.readLong() ;
       _directorySize          = in.readInt() ;
       _directoryBits          = in.readInt() ;
       _avListAddress          = in.readLong() ;
       _magic                  = in.readInt() ;
       if( _magic != MAGIC )
          throw new 
          IOException( "Not a JDBM file" ) ;
       return ;
    }
    public int getPersistentSize() { 
       return _blockSize   ;
    }

}