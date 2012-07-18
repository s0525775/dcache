package dmg.util ;

import java.util.Stack;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class Formats {

  public static final int CENTER   =  0x1 ;
  public static final int RIGHT    =  0x4 ;
  public static final int LEFT     =  0x2 ;
  public static final int CUT      =  0x8 ;

  public static String field( String in , int field ){
     return field(in,field,LEFT) ;
  }
  public static String field( String in , int field , int flags ){
    if( in.length() >= field ){
      if( ( flags & CUT ) > 0 ){
         return in.substring( 0 , field+1 ) ;
      }else{
         return in ;
      }
    }
    StringBuilder sb = new StringBuilder() ;
    int i ;
    if( ( flags & CENTER ) > 0 ){
       int diff  = field - in.length() ;
       int left  = diff / 2 ;
       int right = diff - left ;
       for( i = 0 ; i < left ; i++ ) {
           sb.append(" ");
       }
       sb.append( in ) ;
       for( i = 0 ; i < right ; i++ ) {
           sb.append(" ");
       }
    }else if( ( flags & RIGHT ) > 0 ){
       int diff = field - in.length() ;
       for( i = 0 ; i < diff ; i++ ) {
           sb.append(" ");
       }
       sb.append( in ) ;
    }else{
       sb.append( in ) ;
       int diff = field - in.length() ;
       for( i = 0 ; i < diff ; i++ ) {
           sb.append(" ");
       }
    }
    return sb.toString() ;
  }
  public static String cutClass( String c ){
     int lastDot = c.lastIndexOf( '.' ) ;
     if( ( lastDot < 0 ) || ( lastDot >= ( c.length() - 1 ) ) ) {
         return c;
     }
     return c.substring( lastDot+1 ) ;

  }
  private final static int RP_IDLE   = 0 ;
  private final static int RP_DOLLAR = 1 ;
  private final static int RP_OPENED = 2 ;

    private static String replaceKeywords(String in, Replaceable cb, Stack<String> replaced)
    {
        StringBuilder key = null ;
        StringBuilder out = new StringBuilder();
        int state = RP_IDLE ;
        int len   = in.length() ;
        int braceCount = 0;
        boolean startOfReference = false;

        for( int i = 0 ; i < len ; i++ ){
            char c = in.charAt(i) ;
            switch( state ){
            case RP_IDLE :
                switch(c) {
                case '$':
                    state = RP_DOLLAR;
                    break;
                default:
                    out.append(c);
                }
                break ;

            case RP_DOLLAR :
                switch(c) {
                case '{':
                    startOfReference=false;
                    state = RP_OPENED ;
                    braceCount = 0;
                    key   = new StringBuilder();
                    break;

                case '$':
                    out.append('$');
                    state = RP_IDLE;
                    break;

                default:
                    out.append('$').append(c);
                    state = RP_IDLE ;
                }
                break ;

            case RP_OPENED :
                switch(c) {
                case '$':
                    if(!startOfReference) {
                        key.append(c);
                    }
                    startOfReference = !startOfReference;
                    break;

                case '{':
                    key.append(c);
                    if(startOfReference) {
                        braceCount++;
                        startOfReference=false;
                    }
                    break;

                case '}':
                    startOfReference=false;
                    if(braceCount > 0) {
                        braceCount--;
                        key.append(c);
                    } else {
                        state = RP_IDLE;
                        String keyName = replaceKeywords(key.toString(), cb, replaced);
                        String keyValue = cb.getReplacement(keyName);

                        if (keyValue == null || replaced.contains(keyName)) {
                            out.append("${").append(keyName).append("}");
                        } else {
                            replaced.push(keyName);
                            out.append(replaceKeywords(keyValue, cb, replaced));
                            replaced.pop();
                        }
                    }
                    break;

                default:
                    key.append(c);
                    break;
                }
            }
        }

        switch(state) {
        case RP_DOLLAR:
            out.append('$');
            break;
        case RP_OPENED:
            out.append("${").append(key);
            break;
        }

        return out.toString();
    }

    public static String replaceKeywords(String in, Replaceable cb)
    {
        return replaceKeywords(in, cb, new Stack<String>());
    }

  public static boolean smatch( String pattern , String text ){

    int pl = pattern.length() ;
    int tl = text.length() ;
    int i = 0 ;
    for( ; ( i < pl ) && ( i < tl ) && ( pattern.charAt(i) != '*' ) &&
         ( pattern.charAt(i) == text.charAt(i) ) ; i++ ) {
    }
    if( ( i == pl ) && ( i == tl ) ) {
        return true;
    }
    if( ( i == pl ) || ( i == tl ) ) {
        return false;
    }
    if( pattern.charAt(i) == '*' ) {
        return true;
    }
    return false ;
  }
/**
 * a useful tool which can interpret jokers (*) and wildcards (?) to filter
 * from a given Array of Strings the matching ones.
 * Written by Manfred Maschewski, DESY
 *
 * @author              Manfred Maschewski, DESY Hamburg
 * @version             0.1             14 Jan 99
 */
  public static boolean match( String condition , String subject ) {
    // handling the joker
    if ( condition.startsWith("*") ) {
      // removing the joker
      condition = condition.substring( 1 ) ;
      int nextJoker = condition.indexOf("*") ;
      if ( nextJoker == -1 ) {
        return endsWith( subject, condition ) ;
      } else {
        String firstPart = condition.substring( 0, nextJoker ) ;
        int position = indexOf( subject, firstPart ) ;
        if ( position == -1 ) {
          return false;
        } else {
          condition = condition.substring( firstPart.length() ) ;
          subject = subject.substring( position + firstPart.length() ) ;
          return match( condition , subject  ) ;
        }
      }
    } else {
      int nextJoker = condition.indexOf("*") ;
      if ( nextJoker == -1 ) {
        return equals( subject, condition ) ;
      } else {
        String firstPart = condition.substring( 0, nextJoker ) ;
        if ( ! startsWith( subject, firstPart ) ) {
                return false ;
        } else {
                condition = condition.substring( firstPart.length() ) ;
                subject = subject.substring( firstPart.length() ) ;
                return match( condition , subject ) ;
        }
      }
    }
  }


  private static boolean startsWith( String subject, String term ) {
     int riddle ;
     subject = subject.substring( 0, term.length() ) ;
     while ( ( riddle = term.indexOf("?")) != -1 ) {
       term = removeChar(term,riddle) ;
       subject = removeChar(subject,riddle) ;
     }
     return equals( subject, term ) ;
  }

  private static boolean endsWith( String subject, String term ) {
    int riddle ;
    // if term is longer than subject it doesn't matches anyway...
    if ( subject.length() < term.length() ) {
      return false ;
    }
    subject = subject.substring( subject.length() - term.length() ) ;
    while ( ( riddle = term.indexOf("?")) != -1 ) {
      term = removeChar(term,riddle) ;
      subject = removeChar(subject,riddle) ;
    }
    return equals( subject, term ) ;
  }

  private static int indexOf( String subject, String term ) {
    if ( subject.length() < term.length() ) {
      return -1 ;
    }
    // going throught the subject
    outer: for ( int i = 0 ; i < (subject.length() - term.length() + 1); i++ ){
      for ( int j = 0; j < term.length(); j++ ) {
        if ( ! ((term.charAt(j) == subject.charAt(j+i)))
             && ! (term.charAt(j) == '?') ) {
          continue outer ;
        }
      }
      return i ;
    }
    return -1 ;
  }
  private static String removeChar( String str, int i ) {
    str = str.substring(0,i) + str.substring(i+1,str.length()) ;
    return str ;
  }
  private static boolean equals( String subject, String term ) {
     int riddle ;
     while ( ( riddle = term.indexOf("?")) != -1 ) {
             term = removeChar(term,riddle) ;
             subject = removeChar(subject,riddle) ;
     }
     return subject.equals(term) ;
  }

}
