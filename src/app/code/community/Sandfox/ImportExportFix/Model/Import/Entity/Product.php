<?php
/* NOTICE OF LICENSE
 *
 * This source file is subject to the MIT license
 * that is bundled with this package in the file LICENSE.txt.
 * It is also available through the world-wide-web at this URL:
 * http://opensource.org/licenses/MIT
 *
 * @copyright  Copyright (c) 2014 Tim Bezhashvyly
 * @license    http://opensource.org/licenses/MIT  The MIT License
 */
 
class Sandfox_ImportExportFix_Model_Import_Entity_Product extends Enterprise_ImportExport_Model_Import_Entity_Product
{
    /**
     * Code of a primary attribute which identifies the entity group if import contains of multiple rows
     *
     * @var string
     */
    protected $masterAttributeCode = 'sku';

    /**
     * Validate data rows and save bunches to DB.
     *
     * @return Mage_ImportExport_Model_Import_Entity_Abstract
     */
    protected function _saveValidatedBunches()
    {
        $source          = $this->_getSource();
        $bunchRows       = array();
        $startNewBunch   = false;
        $maxDataSize = Mage::getResourceHelper('importexport')->getMaxDataSize();
        $bunchSize = Mage::helper('importexport')->getBunchSize();

        $source->rewind();
        $this->_dataSourceModel->cleanBunches();

        while ($source->valid() || count($bunchRows) || isset($entityGroup)) {
            if ($startNewBunch || !$source->valid()) {
                /* If the end approached add last validated entity group to the bunch */
                if (!$source->valid() && isset($entityGroup)) {
                    $bunchRows = array_merge($bunchRows, $entityGroup);
                    unset($entityGroup);
                }
                $this->_dataSourceModel->saveBunch($this->getEntityTypeCode(), $this->getBehavior(), $bunchRows);
                $bunchRows = array();
                $startNewBunch = false;
            }
            if ($source->valid()) {
                if ($this->_errorsCount >= $this->_errorsLimit) { // errors limit check
                    return $this;
                }
                $rowData = $source->current();

                $this->_processedRowsCount++;

                if (isset($rowData[$this->masterAttributeCode]) && trim($rowData[$this->masterAttributeCode])) {
                    /* Add entity group that passed validation to bunch */
                    if (isset($entityGroup)) {
                        $bunchRows = array_merge($bunchRows, $entityGroup);
                        $productDataSize = strlen(serialize($bunchRows));

                        /* Check if the nw bunch should be started */
                        $isBunchSizeExceeded = ($bunchSize > 0 && count($bunchRows) >= $bunchSize);
                        $startNewBunch = $productDataSize >= $maxDataSize || $isBunchSizeExceeded;
                    }

                    /* And start a new one */
                    $entityGroup = array();
                }

                if (isset($entityGroup) && $this->validateRow($rowData, $source->key())) {
                    /* Add row to entity group */
                    $entityGroup[$source->key()] = $this->_prepareRowForDb($rowData);
                } elseif (isset($entityGroup)) {
                    /* In case validation of one line of the group fails kill the entire group */
                    unset($entityGroup);
                }
                $source->next();
            }
        }
        return $this;
    }
}
