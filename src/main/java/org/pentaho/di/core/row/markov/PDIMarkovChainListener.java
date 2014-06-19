/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2014 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.pentaho.di.core.row.markov;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.RecognitionException;
import org.pentaho.di.core.row.markov.antlr.PDIMarkovChainBaseListener;
import org.pentaho.di.core.row.markov.antlr.PDIMarkovChainParser;

/**
 * @author mburgess
 * 
 */
public class PDIMarkovChainListener extends PDIMarkovChainBaseListener {

  Map<String, Map<String, List<MarkovHop>>> transMarkovHops = new HashMap<String, Map<String, List<MarkovHop>>>();
  Map<String, List<MarkovHop>> currentHopMap = null;

  String currentTrans = null;
  String currentSourceStep = null;
  MarkovHop currentHop = null;

  @Override
  public void enterTransName( PDIMarkovChainParser.TransNameContext ctx ) {
    if ( currentTrans == null ) {
      currentTrans = unquoteString( ctx.STRING().toString() );

      currentHopMap = new HashMap<String,List<MarkovHop>>();
      transMarkovHops.put( currentTrans, currentHopMap );
    }
  }

  @Override
  public void exitTrans( PDIMarkovChainParser.TransContext ctx ) {
    currentTrans = null;
  }

  @Override
  public void enterProbability( PDIMarkovChainParser.ProbabilityContext ctx ) {
    if ( currentHop != null ) {
      currentHop.setProbability( Float.valueOf( ctx.FLOAT().toString() ) );
    }
  }

  @Override
  public void enterHopExpr( PDIMarkovChainParser.HopExprContext ctx ) {
    currentHop = new MarkovHop();
  }

  @Override
  public void enterStep( PDIMarkovChainParser.StepContext ctx ) {
    if ( currentHop != null ) {
      if ( currentSourceStep == null ) {
        currentSourceStep = unquoteString( ctx.STRING().toString() );
        if(currentHopMap.get( currentSourceStep ) == null) {
          currentHopMap.put( currentSourceStep, new ArrayList<MarkovHop>() );
        } 
      } else {
        currentHop.setTargetStepName( unquoteString( ctx.STRING().toString() ) );
      }
    }
  }

  @Override
  public void exitHopExpr( PDIMarkovChainParser.HopExprContext ctx ) {

    if ( currentHop != null ) {
      List<MarkovHop> hops = currentHopMap.get( currentSourceStep );
      if ( hops != null ) {
        hops.add( currentHop );
        currentHop = null;
        currentSourceStep = null;
      } else {
        throw new PDIMarkovChainParserException( "Cannot associate hop with " + currentSourceStep + " in "
            + currentTrans );
      }
    } else {
      throw new PDIMarkovChainParserException( "Hop should not be null!" );
    }
  }

  public Map<String, Map<String, List<MarkovHop>>> getMarkovHopMap() {
    return transMarkovHops;
  }

  protected String unquoteString( String quotedString ) {
    if ( quotedString == null ) {
      return null;
    }
    if ( quotedString.indexOf( '"' ) == 0 && quotedString.lastIndexOf( '"' ) == quotedString.length() - 1 ) {
      return quotedString.substring( 1, quotedString.length() - 1 );
    }
    return quotedString;
  }

}
