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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.logging.KettleLogStore;

/**
 * @author mburgess
 * 
 */
public class MarkovChainRowDistributionTest {

  public static final int NUM_ROWS_TO_TEST = 10000;
  public static final int[] rowSetSelection = new int[NUM_ROWS_TO_TEST];

  public static final int CANNED_HOP_LIST_SIZE = 4;
  public static final List<MarkovHop> cannedHopList = new ArrayList<MarkovHop>( CANNED_HOP_LIST_SIZE );
  public static final Map<String, List<MarkovHop>> cannedHopMap = new HashMap<String, List<MarkovHop>>();

  MarkovChainRowDistribution mChain;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    KettleLogStore.init();
    for ( int i = 0; i < CANNED_HOP_LIST_SIZE; i++ ) {
      final float p = ( i + 1 ) / 10.0f;
      final int addStepNum = i;
      cannedHopList.add( new MarkovHop() {
        {
          setTargetStepName( "Add " + addStepNum );
          setProbability( p );
        }
      } );
    }
    cannedHopMap.put( "Generate Rows", cannedHopList );
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    mChain = new MarkovChainRowDistribution();
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testGetRowSetWithRandomNumber() {
    float[] testProb = { 0.1f, 0.3f, 0.7f, 1.0f };

    for ( int i = 0; i < NUM_ROWS_TO_TEST; i++ ) {
      rowSetSelection[i] = mChain.getRowSetWithRandomNumber( testProb );
    }

  }

  @Test
  public void testGetUpperProbabilityBounds() {
    try {
      float[] uProb = mChain.getUpperProbabilityBounds( cannedHopList );
      assertNotNull( uProb );
      float sum = 0.0f;
      for ( int i = 0; i < uProb.length; i++ ) {
        sum += ( (float) ( i + 1 ) / 10.0f );
        assertEquals( sum, uProb[i], 0.0000001f );
      }
    } catch ( ProbabilityException e ) {
      fail( "No Exception expected!" );
    }
  }

  @Test( expected = ProbabilityException.class )
  public void testGetUpperProbabilityBoundsWithTooHighProbability() throws ProbabilityException {

    List<MarkovHop> badHopList = new ArrayList<MarkovHop>();
    badHopList.addAll( cannedHopList );
    badHopList.add( new MarkovHop() {
      {
        setProbability( 10.0f );
      }
    } );
    // Should throw an exception
    mChain.getUpperProbabilityBounds( badHopList );
  }

  @Test( expected = ProbabilityException.class )
  public void testGetUpperProbabilityBoundsWithTooLowProbability() throws ProbabilityException {

    List<MarkovHop> badHopList = new ArrayList<MarkovHop>();
    badHopList.addAll( cannedHopList );
    badHopList.remove( 0 );

    // Should throw an exception
    mChain.getUpperProbabilityBounds( badHopList );
  }
}
