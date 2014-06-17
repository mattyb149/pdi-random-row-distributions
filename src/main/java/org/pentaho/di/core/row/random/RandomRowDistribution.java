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
package org.pentaho.di.core.row.random;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.gui.PrimitiveGCInterface.EImage;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.RowDistributionInterface;
import org.pentaho.di.trans.step.RowDistributionPlugin;
import org.pentaho.di.trans.step.StepInterface;

/**
 * @author mburgess
 * 
 */

@RowDistributionPlugin( code = "Random", name = "Random",
    description = "Distributes to each row with equal probability" )
public class RandomRowDistribution implements RowDistributionInterface {

  /**
   * @see org.pentaho.di.trans.step.RowDistributionInterface#distributeRow(org.pentaho.di.core.row.RowMetaInterface,
   *      java.lang.Object[], org.pentaho.di.trans.step.StepInterface)
   */
  @Override
  public void distributeRow( RowMetaInterface rowMeta, Object[] row, StepInterface step ) throws KettleStepException {

    List<RowSet> rowSets = step.getOutputRowSets();
    int numRowSets = rowSets.size();

    int selectedRowSet = (int) ( Math.random() * numRowSets );

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
    return "Random";
  }

  /**
   * @see org.pentaho.di.trans.step.RowDistributionInterface#getDescription()
   */
  @Override
  public String getDescription() {
    return "Distributes to each row with equal probability";
  }

  /**
   * @see org.pentaho.di.trans.step.RowDistributionInterface#getDistributionImage()
   */
  @Override
  public EImage getDistributionImage() {
    // TODO Auto-generated method stub
    return null;
  }

}
