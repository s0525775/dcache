package dmg.cells.services.gui ;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;

import dmg.cells.network.CellDomainNode;
import dmg.cells.nucleus.CellDomainInfo;
import dmg.cells.nucleus.CellTunnelInfo;


public class CanonicalTopology {


   private class LinkPair2 implements Comparable<LinkPair2> {
      int [] _pair = new int [2] ;

       public LinkPair2( int x , int y ){
         if( x > y ){ _pair[0] = y ; _pair[1] = x ; }
         else       { _pair[0] = x ; _pair[1] = y ; }
       }
       public boolean equals( Object obj ){
    	   boolean equals = false;
    	   if( (obj instanceof LinkPair2 ) &&
    			   ( _pair[0] == ((LinkPair2)obj)._pair[0] ) &&
    			   ( _pair[1] == ((LinkPair2)obj)._pair[1] )    ) {

    		   equals = true ;
    	   }

    	   return equals;
       }
       public int hashCode(){
          return ( _pair[0] << 16 ) | _pair[1] ;
       }
       @Override
       public int compareTo( LinkPair2 x ){
          if( ( _pair[0] == x._pair[0] ) &&
              ( _pair[1] == x._pair[1] )     ) {
              return 0;
          }
          if( ( _pair[0] < x._pair[0]  ) ||
             (( _pair[0] == x._pair[0] ) &&
              ( _pair[1] < x._pair[1] ) ) ) {
              return -1;
          }
          return 1 ;
       }
       public String toString(){
          return " [0]="+_pair[0]+" [1]="+_pair[1] ;
       }
       public int getBottom(){ return _pair[0] ; }
       public int getTop(){ return _pair[1] ; }
   }
   private String   []  _domainNames;
   private LinkPair2 []  _linkPairs;

   /**
    *   The CanonTopo helper class created a canonical form
    *   of a topology. This makes it possible to compare
    *   different topoligies which are essentially identical.
    */
   public CanonicalTopology(){}
   public CanonicalTopology( CellDomainNode [] nodes ){

      _domainNames = new String[nodes.length] ;
      //
      // copy the domain names into _domainNames
      //
      for( int i = 0 ; i < nodes.length ; i++ ) {
          _domainNames[i] = nodes[i].getName();
      }
      //
      // get some kind of order ( canonical ) into names
      //
      Arrays.sort( _domainNames ) ;
      //
      // produce a 'name to index' hash
      //
      Hashtable<String, Integer> nameHash = new Hashtable<>() ;

      for( int i= 0 ; i < nodes.length ; i++ ) {
          nameHash.put(_domainNames[i], i);
      }
      //
      // produce the 'link hash'
      // the hashtable will essentially remove
      // the duplicated entries.
      //
      Hashtable<LinkPair2, LinkPair2> linkHash = new Hashtable<>() ;

       for (CellDomainNode node : nodes) {

           String thisDomain = node.getName();
           int thisPosition = nameHash.get(thisDomain);
           CellTunnelInfo[] links = node.getLinks();
           if (links == null) {
               continue;
           }

           for (CellTunnelInfo link : links) {
               CellDomainInfo info = link.getRemoteCellDomainInfo();
               if (info == null) {
                   continue;
               }
               String thatDomain = info.getCellDomainName();
               int thatPosition = nameHash.
                       get(thatDomain);
               LinkPair2 pair = new LinkPair2(thisPosition, thatPosition);
               linkHash.put(pair, pair);
           }

       }
      _linkPairs    = new LinkPair2[linkHash.size()] ;
       Iterator<LinkPair2> iterator = linkHash.values().iterator();
      for( int i = 0  ; iterator.hasNext(); i++ ){
         _linkPairs[i] = iterator.next();
      }
      Arrays.sort( _linkPairs ) ;
   }
   public int links(){ return _linkPairs.length ; }
   public int domains(){ return _domainNames.length ; }
   public String getDomain( int i ){ return _domainNames[i] ; }
   public LinkPair2 getLinkPair2( int i ){ return _linkPairs[i] ; }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof CanonicalTopology)) {
            return false;
        }
        CanonicalTopology topo = (CanonicalTopology) other;
        return
            Arrays.equals(_domainNames, topo._domainNames) &&
            Arrays.equals(_linkPairs, topo._linkPairs);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(_domainNames) ^ Arrays.hashCode(_linkPairs);
    }
}
