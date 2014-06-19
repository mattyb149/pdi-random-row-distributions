/*! ******************************************************************************
 *
 * Markov Chain Row Distribution
 *
 * Copyright (C) 2014 by Matt Burgess
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.gui.PrimitiveGCInterface.EImage;
import org.pentaho.di.core.logging.HasLogChannelInterface;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.markov.antlr.PDIMarkovChainLexer;
import org.pentaho.di.core.row.markov.antlr.PDIMarkovChainParser;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.RowDistributionInterface;
import org.pentaho.di.trans.step.RowDistributionPlugin;
import org.pentaho.di.trans.step.StepInterface;

/**
 * @author mburgess
 * 
 */

@RowDistributionPlugin( code = "Markov", name = "Markov chain",
    description = "Distributes to each row with per-hop probability" )
public class MarkovChainRowDistribution implements RowDistributionInterface, HasLogChannelInterface {

  public static final String MARKOV_HOP_FILENAME = "markov-hops.txt";

  public static final String MARKOV_HOP_FILE_LOCATION_PROPERTY = "markov.hopfile.location";

  /** The log channel interface. */
  protected LogChannelInterface log;

  /** The log level. */
  protected LogLevel logLevel = LogLevel.BASIC;

  protected float[] upperProbabilityBound;
  protected Map<String, Map<String, List<MarkovHop>>> markovHopMap;
  protected Map<String, List<MarkovHop>> currentMarkovHopMap;
  protected List<MarkovHop> currentMarkovHops;
  protected String transName;
  protected String stepName;

  public MarkovChainRowDistribution() {
    this.log = new LogChannel( this, null );
    this.logLevel = log.getLogLevel();

    if ( log.isDetailed() ) {
      log.logDetailed( "Loading Markov hop map from " + MARKOV_HOP_FILENAME );
    }
    try {
      markovHopMap = readMarkovHopMap();
      log.logBasic( "Loaded Markov hop map from " + MARKOV_HOP_FILENAME );
    } catch ( Throwable t ) {
      log.logError( "Error loading " + MARKOV_HOP_FILENAME, t );
    }
  }

  /**
   * @see org.pentaho.di.trans.step.RowDistributionInterface#distributeRow(org.pentaho.di.core.row.RowMetaInterface,
   *      java.lang.Object[], org.pentaho.di.trans.step.StepInterface)
   */
  @Override
  public void distributeRow( RowMetaInterface rowMeta, Object[] row, StepInterface step ) throws KettleStepException {

    String lastTransName = transName;

    transName = step.getTrans().getTransMeta().getName();

    if ( !transName.equals( lastTransName ) ) {
      
      currentMarkovHopMap = markovHopMap.get( transName );

      if ( currentMarkovHopMap == null ) {
        throw new KettleStepException( "No hops found for " + transName );
      }
      stepName = null;
      currentMarkovHops = null;
    }
    
    // Get hops for source step
    String lastStepName = stepName;
    stepName = step.getStepname();
    if(!stepName.equals( lastStepName ) || currentMarkovHops == null) { 
      currentMarkovHops = currentMarkovHopMap.get( stepName );
      if ( currentMarkovHops == null ) {
        throw new KettleStepException( "No hops found for " + stepName );
      }
      
      try {  
        upperProbabilityBound = getUpperProbabilityBounds( currentMarkovHops );
      } catch ( ProbabilityException pe ) {
        throw new KettleStepException( pe );
      }
    }
    

    List<RowSet> rowSets = step.getOutputRowSets();
    int numRowSets = rowSets.size();

    // Use random number to select hop, then find corresponding RowSet and send the row
    int selectedRowSet = getRowSetWithRandomNumber( upperProbabilityBound );

    if ( selectedRowSet < 0 ) {
      throw new KettleStepException( "Couldn't select hop" );
    }

    step.setCurrentOutputRowSetNr( selectedRowSet );

    RowSet rowSet = step.getOutputRowSets().get( step.getCurrentOutputRowSetNr() );

    boolean added = false;
    while ( !added ) {
      added = rowSet.putRowWait( rowMeta, row, 1, TimeUnit.NANOSECONDS );
      if ( added ) {
        break;
      }
    }

  }

  /**
   * @see org.pentaho.di.trans.step.RowDistributionInterface#getCode()
   */
  @Override
  public String getCode() {
    return "Markov";
  }

  /**
   * @see org.pentaho.di.trans.step.RowDistributionInterface#getDescription()
   */
  @Override
  public String getDescription() {
    return "Distributes to each row with per-hop probability";
  }

  /**
   * @see org.pentaho.di.trans.step.RowDistributionInterface#getDistributionImage()
   */
  @Override
  public EImage getDistributionImage() {
    // TODO
    return null;
  }

  protected Map<String, Map<String, List<MarkovHop>>> readMarkovHopMap() throws IOException {

    File pluginFolderFile =
        new File( getClass().getProtectionDomain().getCodeSource().getLocation().getPath() ).getParentFile();
    String hopFilePath = System.getProperty( MARKOV_HOP_FILE_LOCATION_PROPERTY, pluginFolderFile.getAbsolutePath() );

    File markovHopFile = new File( hopFilePath, MARKOV_HOP_FILENAME );

    if ( markovHopFile.exists() ) {

      ANTLRInputStream input = new ANTLRInputStream( new FileInputStream( markovHopFile ) );
      PDIMarkovChainLexer lexer = new PDIMarkovChainLexer( input );
      CommonTokenStream tokens = new CommonTokenStream( lexer );
      PDIMarkovChainParser parser = new PDIMarkovChainParser( tokens );
      ParseTreeWalker walker = new ParseTreeWalker();
      PDIMarkovChainListener listener = new PDIMarkovChainListener();
      try {
        walker.walk( listener, parser.prog() );
      } catch ( Throwable t ) {
        log.logError( "Error parsing file: " + markovHopFile.getAbsolutePath(), t );
      }
      return listener.getMarkovHopMap();
    } else {
      throw new FileNotFoundException( "No such file at: " + markovHopFile.getAbsolutePath() );
    }
  }

  protected float[] getUpperProbabilityBounds( List<MarkovHop> markovHops ) throws ProbabilityException {
    float[] uProb = null;
    if ( markovHops != null ) {
      uProb = new float[markovHops.size()];
      float currentBound = 0.0f;
      int index = 0;
      for ( MarkovHop hop : markovHops ) {
        currentBound += hop.getProbability();
        if ( currentBound > 1.0f ) {
          throw new ProbabilityException( "Hop probabilities cannot exceed 1.0" );
        }
        uProb[index++] = currentBound;
      }
      if ( currentBound < 1.0f ) {
        throw new ProbabilityException( "Hop probabilities must sum to 1.0" );
      }
    }
    return uProb;
  }

  /**
   * @param upperProbabilityBound2
   * @return
   */
  protected int getRowSetWithRandomNumber( float[] uProb ) {
    if ( uProb == null ) {
      return -1;
    }

    int i = 0;
    float randomNumber = (float) Math.random();

    while ( i < uProb.length && randomNumber > uProb[i] ) {
      i++;
    }

    return ( i == uProb.length ) ? -1 : i;
  }

  /**
   * @see org.pentaho.di.core.logging.HasLogChannelInterface#getLogChannel()
   */
  @Override
  public LogChannelInterface getLogChannel() {
    return log;
  }
}
