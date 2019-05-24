/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.si;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * Describe your step plugin.
 * 
 */
public class HashPDIPlugin extends BaseStep implements StepInterface {
  private HashPDIPluginMeta meta;
  private HashPDIPluginData data;

  private static Class<?> PKG = HashPDIPluginMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  
  public HashPDIPlugin( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }
  
  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param stepMetaInterface
   *          The metadata to work with
   * @param stepDataInterface
   *          The data to initialize
   */
  public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    data = (HashPDIPluginData) stepDataInterface;
    meta = (HashPDIPluginMeta) stepMetaInterface;
    return super.init( stepMetaInterface, stepDataInterface );
  }

  /**
   * Hash the text using murmurhash3
   *
   * @param text        The text to hash
   * @return            The hash long
   */
  private long murmurhash3(String text){
    int seed = meta.getSeedValue().intValue();
    if(seed <= 0){
      seed = 1073741823;
    }
    HashFunction hfunc = Hashing.murmur3_128(seed);
    HashCode hcode = hfunc.hashBytes(text.getBytes());
    return hcode.asLong();
  }

  /**
   * Hash the rows.
   *
   * @param r       The row to hash
   * @return        The updated object array row
   */
  private Object[] hashRow(Object[] r){
    int idx = data.outputRowMeta.indexOfValue(meta.getInField());
    int oidx = data.outputRowMeta.indexOfValue(meta.getOutField());
    if(idx >= 0 && oidx >= 0){
      String text = (String) r[idx];
      long hashCode = murmurhash3(text);
      r[oidx] = hashCode;
    }else{
      if(isBasic()){
        logBasic("No Input and/or Output Field for Hash");
      }
    }
    return r;
  }

  /**
   * Setup the processor.
   *
   * @throws KettleException
   */
  private void setupProcessor() throws KettleException{
    RowMetaInterface inMeta = getInputRowMeta().clone();
    data.outputRowMeta = inMeta;
    meta.getFields(data.outputRowMeta, getStepname(), null, null, this, null, null);
    first = false;
  }

  /**
   * Process the object row
   *
   * @param smi                   The step meta interface
   * @param sdi                   The step data interface
   * @return                      Whether the row was processed
   * @throws KettleException
   */
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) {
      // no more input to be expected...
      setOutputDone();
      return false;
    }

    if(first){
      setupProcessor();
    }

    if(data.outputRowMeta.size() > r.length){
      r = RowDataUtil.resizeArray(r, data.outputRowMeta.size());
    }

    r = hashRow(r);
    putRow(data.outputRowMeta, r );

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() )
        logBasic( BaseMessages.getString( PKG, "HashPDIPlugin.Log.LineNumber" ) + getLinesRead() );
    }
      
    return true;
  }
}