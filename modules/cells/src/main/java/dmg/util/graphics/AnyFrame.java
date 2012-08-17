package dmg.util.graphics ;

import java.lang.reflect.* ;
import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;
import java.net.* ;

import dmg.util.* ;
import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;

public class      AnyFrame 
       extends    Frame 
       implements WindowListener, ActionListener   {

   private Font   _bigFont = 
             new Font( "SansSerif" , Font.BOLD , 18 )  ; 
   private Font   _bigFont2 = 
             new Font( "SansSerif" , Font.BOLD , 24 )  ; 

  private class NicePanel extends Panel {
     private NicePanel( int columns ){
        TableLayout tl = new TableLayout( columns ) ;
        tl.setHgap(20) ;
        tl.setVgap(10) ;
        setLayout( tl ) ;
     }
     public Insets getInsets(){ return new Insets( _b , _b ,_b , _b ) ; }
     private int _b = 7 ;
     public void paint( Graphics g ){
        Dimension   d    = getSize() ;
        Color base = getBackground() ;
        g.setColor( Color.blue ) ;
        g.drawRect( _b/2 , _b/2 , d.width-_b , d.height-_b ) ;
     }
  }
  public AnyFrame(  ){
      super( "Any Frame" ) ;
      setLayout( new dmg.cells.applets.login.CenterLayout() ) ;
      addWindowListener( this ) ;
      setLocation( 60 , 60) ;
      
      Panel p = new NicePanel( 3 ) ;
      p.setBackground( Color.yellow ) ;
      add( p , "South") ;
      Label x = null ;
      p.add( x = new Label( "1" , Label.CENTER )  ) ;
      x.setBackground( Color.green ) ;
      p.add( x = new Label( "11" , Label.CENTER )  ) ;
      x.setFont( _bigFont ) ;
      x.setBackground( Color.green ) ;
      p.add( x = new Label( "111" , Label.CENTER )  ) ;
      x.setBackground( Color.green ) ;
      p.add( x = new Label( "1111" , Label.CENTER )  ) ;
      x.setBackground( Color.green ) ;
      p.add( new Label( "11111" , Label.CENTER )  ) ;
      p.add( new Label( "11111" , Label.CENTER )  ) ;
      p.add( x = new Label( "11111" , Label.CENTER )  ) ;
      x.setFont( _bigFont2 ) ;
      x.setBackground( Color.green ) ;
      
      TreeCanvas tc = new TreeCanvas() ;
      /*
      TreeNodeImpl otto = new TreeNodeImpl( "Otto" ) ;
      TreeNodeImpl karl = new TreeNodeImpl( "karl" ) ;
      TreeNodeImpl fritz = new TreeNodeImpl( "fritz-is-a-very-long-word" ) ;
      TreeNodeImpl karlNext = new TreeNodeImpl( "karlNext" ) ;
      TreeNodeImpl ottoNext = new TreeNodeImpl( "OttoNext" ) ;
      TreeNodeImpl ottoNextNext = new TreeNodeImpl( "OttoNextNext" ) ;
      otto._next = ottoNext ;
      ottoNext._next = ottoNextNext ;
      otto._sub = karl  ;
      karl._next = karlNext ;
      karl._sub = fritz ;
      ottoNextNext._sub = karl ;
      ottoNextNext._next = new TreeNodeImpl( "last" ) ;
      tc.setTree(otto) ;
      */
      
      
      FileTreeNode ftn = new FileTreeNode( new File("/etc") ) ;
      /*
      tc.setTree(ftn);
      &/
      ScrollPane sp = new ScrollPane() ;
      sp.add( tc ) ;
      sp.setSize( 200 , 300  ) ;
      p.add( sp ) ;
      */
      
      TreeNodePanel tnp = new TreeNodePanel() ;
      tnp.setTree( ftn ) ;
      tnp.setSize( 300 , 300 ) ;
      p.add( tnp ) ;
      
//      p.add( new Label( "Medium-3" )  ) ;
//      p.add( new Label( "Bottom-1" )  ) ;
//      p.add( new Label( "Bottom-2" )  ) ;
      
      setSize( 750 , 500 ) ;
      pack() ;
      setSize( 750 , 500 ) ;
      setVisible( true ) ;
  
  }
  private int _counter = 1000 ;
  public void actionPerformed( ActionEvent event ){
  }
  //
  // window interface
  //
  public void windowOpened( WindowEvent event ){}
  public void windowClosed( WindowEvent event ){
      System.exit(0);
  }
  public void windowClosing( WindowEvent event ){
      System.exit(0);
  }
  public void windowActivated( WindowEvent event ){}
  public void windowDeactivated( WindowEvent event ){}
  public void windowIconified( WindowEvent event ){}
  public void windowDeiconified( WindowEvent event ){}
   public static void main( String [] args ){
      try{
            
         new AnyFrame() ;
      
      }catch( Exception e ){
         e.printStackTrace() ;
         System.err.println( "Connection failed : "+e.toString() ) ;
         System.exit(4);
      }
      
   }

}