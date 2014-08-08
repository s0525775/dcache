package dmg.util ;

public class Gate {
   private boolean _isOpen = true ;
   public Gate(){}
   public Gate( boolean isOpen ){
      _isOpen = isOpen ;
   }

   public synchronized boolean await(long millis) throws InterruptedException
   {
       long deadline = System.currentTimeMillis() + millis;
       while (!_isOpen && deadline > System.currentTimeMillis()) {
           wait(deadline - System.currentTimeMillis());
       }
       return _isOpen;
   }

   public synchronized Object check(){
      while( true ){
         if( _isOpen ) {
             return this;
         }
         try{ wait() ; }catch( Exception ee ){}
      }

   }

   public synchronized boolean isOpen() {
       return _isOpen;
   }

   public synchronized void open(){
     _isOpen = true ;
     notifyAll() ;
   }
   public synchronized void close(){
     _isOpen = false ;
     notifyAll() ;
   }

}
