/*
 * Copyright (c) 2017 WeServe LLC. All Rights Reserved.
 *
 */


import com.navis.argo.ArgoConfig
import com.navis.argo.ArgoEntity
import com.navis.argo.ArgoExtractEntity
import com.navis.argo.ArgoExtractField
import com.navis.argo.ArgoField
import com.navis.argo.ArgoIntegrationEntity
import com.navis.argo.ArgoJobEntity
import com.navis.argo.ArgoRefEntity
import com.navis.argo.ArgoRefField
import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.atoms.LocTypeEnum
import com.navis.argo.business.atoms.DataSourceEnum
import com.navis.argo.business.atoms.LogicalEntityEnum
import com.navis.argo.business.atoms.ServiceOrderStatusEnum
import com.navis.argo.business.atoms.ServiceOrderUnitStatusEnum
import com.navis.argo.business.model.GeneralReference
import com.navis.cargo.InventoryCargoEntity
import com.navis.cargo.InventoryCargoField
import com.navis.control.ControlEntity
import com.navis.edi.EdiEntity
import com.navis.edi.ServicesEdiEventsEntity
import com.navis.external.argo.AbstractGroovyJobCodeExtension
import com.navis.external.framework.AbstractExtensionCallback
import com.navis.external.framework.util.ExtensionUtils
import com.navis.framework.business.Roastery
import com.navis.framework.metafields.MetafieldId
import com.navis.framework.persistence.DatabaseHelper
import com.navis.framework.persistence.Entity
import com.navis.framework.persistence.HiberCache
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.persistence.hibernate.CarinaPersistenceCallback
import com.navis.framework.persistence.hibernate.PersistenceTemplate
import com.navis.framework.portal.FieldChanges
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.UserContext
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.util.BizFailure
import com.navis.inventory.InventoryEntity
import com.navis.inventory.InventoryField
import com.navis.inventory.MovesEntity
import com.navis.inventory.MovesField
import com.navis.inventory.ServicesMovesEntity
import com.navis.mensa.MensaEntity
import com.navis.mensa.MensaField
import com.navis.mensa.MensalyticsEntity
import com.navis.orders.OrdersEntity
import com.navis.orders.OrdersField
import com.navis.rail.RailEntity
import com.navis.road.RoadApptsEntity
import com.navis.road.RoadApptsField
import com.navis.road.RoadArchiveApptsEntity
import com.navis.road.RoadArchiveApptsField
import com.navis.road.RoadEntity
import com.navis.road.RoadField
import com.navis.road.business.util.RoadBizUtil
import com.navis.services.ServicesEntity
import com.navis.services.ServicesField
import com.navis.vessel.VesselEntity
import com.navis.yard.YardEntity
import com.navis.yard.YardField
import org.apache.log4j.Logger
import org.hibernate.Query
import org.hibernate.classic.Session

/*
 *
 * Description : This groovy class will selectively purge the working data based on general reference Specialpurge
 *    a.  Special case - * - This would purge all the records in all transactional entities
 * 	  b.  Special case - ! - This would delete the EXTRACT DATA from general reference.
 *
 *  @Inclusion Location	: Incorporated as a Code Extensions as mention below.
 *
 * Deployment Steps:
 *	a) Administration -> System -> Code Extensions
 *	b) Click on + (Add) Button
 *	c) Add the code  name as VITSpecialPurgeWorkingData and type as GROOVY_JOB_CODE_EXTENSION
 *	d) Paste the groovy code and click on save
 *
 * @Set up Groovy Job for this code (SPECIAL_PURGE) and schedule it accordingly.
 *
 *
 */

public class VITSpecialPurgeWorkingData extends  AbstractGroovyJobCodeExtension{
    public void execute(Map<String, Object> inParameters) {
        _dbHelper = (DatabaseHelper) Roastery.getBean(DatabaseHelper.BEAN_ID);
        UserContext userContext = ContextHelper.getThreadUserContext();
        //Set the groovy log name
        groovyLog = ExtensionUtils.getLibrary(userContext, "groovyLog");
        groovyLog.setLogName(this.getClass().getSimpleName());

        GeneralReference generalReference = GeneralReference.findUniqueEntryById(DATAMIGRATION, SPECIALPURGE)
        if (generalReference == null || generalReference.getRefId2() == null) {
            logMessage("General Reference for Type:DATAMIGRATION, Id1:SPECIALPURGE Not Configured")
            throw (BizFailure.create("Please configure General Reference and its value for Type:DATAMIGRATION, Id1:SPECIALPURGE"))
        }
        //The input ref value would be case insensitive. Modifying it to uppercase.
        String getRefId2 = generalReference.getRefId2().toUpperCase();
        String[] getWorkingData;
        if (getRefId2.contains("*")) {
            getWorkingData = dataPurge.toCharArray();
        } else {
            getWorkingData = getRefId2.toCharArray();
        }
        final PersistenceTemplate pt = new PersistenceTemplate(userContext)
        invokePurgeData(getWorkingData, pt)
        if (getRefId2.contains("*")) {
            purgeOtherEntities(pt)
        }
    }

    /**
     * This method would refer to the static maps and nullify/update the status/ purge the entities accordingly.
     **/
    void invokePurgeData(String[] inData, PersistenceTemplate inPt) {
        long startTime = System.currentTimeMillis();
        logMessage("SpecialPurgeWorkingData: Starting ******")
        for (int i = 0; i < inData.length; i++) {
            if (!purgeWorkingDataHelper().containsKey(inData[i]) && inData[i] != "!") {
                logMessage("Code Configured in General Reference: " + inData[i] + " is unknown ")
            } else if (inData[i] == "!") { // if the input contains exclamatory, purge the last extract data
                logMessage("Data to be purged for " + inData[i])
                purgeLastExtractData();
            } else {// this would nullify and purge entities
                logMessage("Data to be purged for " + inData[i] + "*********")
                if (nullWorkingDataHelper().containsKey(inData[i])) {//checks for the input char in nullify map
                    Map nullifyMap = nullWorkingDataHelper().get(inData[i]);
                    for (Map.Entry<String, List> entry : nullifyMap.entrySet()) {
                        String key = entry.getKey();
                        List<MetafieldId> value = entry.getValue();
                        for (MetafieldId nullData : value) {
                            clearRefFieldInEntity(key, nullData, inPt);
                        }
                    }
                }

                //Clear the carrier visit reference for all the visit(Truck,Rail,Vessel)
                purgeVisit(inData[i], inPt)
                //few entities require status change
                checkForStatusChange(inData[i], inPt);

                List purgeData = purgeWorkingDataHelper().get(inData[i]);
                // this would fetch the entities from map
                for (String purgeMethod : purgeData) {
                    callPurgeEntity(inData[i], purgeMethod, inPt);
                }
                //Purge event for few entities
                purgeEventHelper(inData[i], inPt);
            }
            logMessage("*******************")
        }
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        logMessage("Execution time(in H:M:S) is " + ArgoUtils.timeSpanToString(executionTime))
        logMessage("SpecialPurgeWorkingData: End******")
    }

    /**
     * The method would purge Last extract date from general reference
     */
    void purgeLastExtractData() {
        List generalReference = GeneralReference.findAllEntriesById(DATAMIGRATION, LASTEXTRACTDATE)
        logMessage("SpecialPurgeWorkingData: purge " + generalReference.size() + " LastextractDate")
        if (generalReference != null || !generalReference.isEmpty()) {
            generalReference.each {
                HibernateApi.getInstance().delete(it, true)
            }
        }

    }

    /**
     * This would call the domain query and nullify the corresponding field in the entities
     */
    void clearRefFieldInEntity(String inKey, MetafieldId inValue, PersistenceTemplate pt) {
        DomainQuery dq = QueryUtils.createDomainQuery(inKey)
                .addDqPredicate(PredicateFactory.isNotNull(inValue))
        dq.setScopingEnabled(Boolean.FALSE);
        clearAllReferenceInEntity(dq, inValue, pt)
    }

    /**
     * Purge the entity with the domain query
     * @param data Element mapping of the entity
     * @param inEntityName The entity to be purged
     * @param inPt
     */
    private void callPurgeEntity(String data, final String inEntityName, PersistenceTemplate inPt) {
        final Object[] gkeyHolder = new Object[1]
        inPt.invoke(new CarinaPersistenceCallback() {
            @Override
            protected void doInTransaction() {
                ContextHelper.setThreadDataSource(DataSourceEnum.USER_DBA)
                DomainQuery dq = QueryUtils.createDomainQuery(inEntityName);
                //For TBD Units purge the corresponding WI
                if ((data == "H") && inEntityName == MovesEntity.WORK_INSTRUCTION) {
                    dq.addDqPredicate(PredicateFactory.isNotNull(MovesField.WI_TBD_UNIT))
                }
                dq.setScopingEnabled(Boolean.FALSE);
                //Currently created Integration error were not getting purged. Separate method to purge errors
                if (ArgoIntegrationEntity.INTEGRATION_ERROR == inEntityName) {
                    gkeyHolder[0] = batchSelect(inEntityName)
                } else {
                    gkeyHolder[0] = HibernateApi.getInstance().findPrimaryKeysByDomainQuery(dq);
                }
            }
        })

        Serializable[] keys = (Serializable[]) gkeyHolder[0]
        if (keys.length > 0) {
            try {
                logMessage("SpecialPurgeWorkingData: Starting to Purge entity: " + inEntityName);
                purgeAllValue(inEntityName, (Serializable[]) gkeyHolder[0], inPt)
            } catch (Exception e) {
                LOGGER.error("Could not delete entities due to the : " + e.getMessage());
            }
        }
        // For logging purpose
        int totalCount = getCount(inEntityName);
        if (totalCount != 0 && keys.length > 0 && !((data == "H") && inEntityName == MovesEntity.WORK_INSTRUCTION)) {
            logMessage("WARN: SpecialPurgeWorkingData: Purge failure - " + totalCount + " " + inEntityName + "'s")
            if (keys.length != totalCount) {
                logMessage("SpecialPurgeWorkingData: PurgeEntity: Purged " + (keys.length - totalCount) + " " + inEntityName + "'s")
            }
        } else {
            logMessage("SpecialPurgeWorkingData: PurgeEntity: Purged " + keys.length + " " + inEntityName + "'s")
        }
    }

    /**
     * Based on the primary key, delete the entities
     * @param inEntityName entity to be purged
     * @param inPrimaryKeys The records Pkey List
     * @param inPt
     */
    private static void purgeAllValue(String inEntityName, final Serializable[] inPrimaryKeys,
                                      PersistenceTemplate inPt) {
        int entitiesDeleted = 0

        // Delete each entity
        final PurgeEachDataByEntity purgeEachEntity = new PurgeEachDataByEntity(inEntityName)
        for (int i = 0; i < inPrimaryKeys.length; i++) {
            purgeEachEntity.setPrimaryKey(inPrimaryKeys[i])
            inPt.invoke(purgeEachEntity)
            entitiesDeleted++
        }
    }

    /**
     * Null entity based on the domain query
     * @param inDq domain query
     * @param inMetafieldId Field to be nullified
     * @param inPt
     */
    private void clearAllReferenceInEntity(
            final DomainQuery inDq,
            final MetafieldId inMetafieldId, PersistenceTemplate inPt) {

        Serializable[] pKeys = HibernateApi.getInstance().findPrimaryKeysByDomainQuery(inDq)
        String entityName = inDq.getEntityId().getEntityName()
        if (pKeys != null) {
            logMessage("SpecialPurgeWorkingData: null entities: " + pKeys.length + " " + entityName + "'s field " + inMetafieldId.getFieldId())
            for (int i = 0; i < pKeys.length; i++) {
                clearReferenceKey(entityName, pKeys[i], inMetafieldId, inPt)
            }
        }
    }

    /**
     * Clear the records based on the pkey and the field
     * @param inEntityName
     * @param inPrimaryKey
     * @param inMetafieldId
     * @param pt
     */
    private static void clearReferenceKey(String inEntityName,
                                          final Serializable inPrimaryKey,
                                          final MetafieldId inMetafieldId,
                                          PersistenceTemplate pt) {

        final NullKeyTask updateNullInEntity = new NullKeyTask(inEntityName)
        updateNullInEntity.setPrimaryKey(inPrimaryKey)
        updateNullInEntity.setForiengKey(inMetafieldId)
        pt.invoke(updateNullInEntity)
    }

    /**
     * The status for certain entities should be set to New
     *
     * @param inData the working data character
     * @param inPt Persistence template
     */
    private void checkForStatusChange(String inData, PersistenceTemplate inPt) {
        switch (inData) {
            case "P":
                updateOrderStatus(inPt, SERVICEORDER);
                updateOrderStatus(inPt, SERVICETYPE);
                break;
            case "R":
                updateOrderStatus(inPt, CARGOSERVICEORDER);
                break;
            case "X":
                updateOrderStatus(inPt, CVSERVICEORDER);
                break;
            case "A":
                purgeEquipmentState(inPt);
                updateOrderStatus(inPt, SERVICETYPE);
                break;
            case "Q":
            case "3":
                updateOrderStatus(inPt, SERVICETYPE);
                break;
            default:
                break;
        }
    }

    /**
     * Purge corresponding Events for Entities namely Unit, VV, SO and so on based on the purgeEventHelper()
     * @param inTargetLogicalEntity
     * @param inPt
     */
    private void purgeEvents(LogicalEntityEnum inTargetLogicalEntity, PersistenceTemplate inPt) {
        final Object[] gkeyHolder = new Object[1]
        inPt.invoke(new CarinaPersistenceCallback() {
            @Override
            protected void doInTransaction() {
                ContextHelper.setThreadDataSource(DataSourceEnum.USER_DBA)
                DomainQuery domainQuery = QueryUtils.createDomainQuery(ServicesEntity.EVENT)
                        .addDqPredicate(PredicateFactory.eq(ServicesField.EVNT_APPLIED_TO_CLASS, inTargetLogicalEntity))
                domainQuery.setScopingEnabled(Boolean.FALSE);
                gkeyHolder[0] = HibernateApi.getInstance().findPrimaryKeysByDomainQuery(domainQuery)
            }
        })
        Serializable[] findEvent = (Serializable) gkeyHolder[0]
        if (findEvent.length > 0) {
            try {
                purgeAllValue(ServicesEntity.EVENT, (Serializable[]) gkeyHolder[0], inPt)
            } catch (Exception e) {
                LOGGER.error("Could not delete entities due to the : " + e.getMessage());
            }
        }
        logMessage("SpecialPurgeWorkingData: Purged " + findEvent.length + " " + inTargetLogicalEntity.getKey() + " Event's")
    }

    /**
     * Purge Unit's equipment state based on the condition DELETE_WORKING_DATA_SKIP_EQUIPMENT_STATE
     * @param inPt
     */
    private void purgeEquipmentState(PersistenceTemplate inPt) {
        UserContext uc = ContextHelper.getThreadUserContext();
        if (ArgoConfig.DELETE_WORKING_DATA_SKIP_EQUIPMENT_STATE.isOn(uc)) {
            clearRefFieldInEntity(InventoryEntity.EQUIPMENT_STATE, InventoryField.EQS_LAST_POS_NAME, inPt);
        } else {
            callPurgeEntity("A", InventoryEntity.EQUIPMENT_STATE, inPt);
        }
    }

    /**
     * This is applicable while purging through "*"
     * @param inPt
     */
    private void purgeOtherEntities(PersistenceTemplate inPt) {
        logMessage("SpecialPurgeWorkingData: Start Purging other entities ******")
        final Map<String, List<MetafieldId>> nullifyEntity = new HashMap<String, List<MetafieldId>>();
        nullifyEntity.put(ArgoRefEntity.CARRIER_ITINERARY, Arrays.asList(ArgoRefField.ITIN_OWNER_CV))
        nullifyEntity.put(YardEntity.SWAP_HISTORY, Arrays.asList(YardField.SH_SOURCE_CARRIER_VISIT,
                YardField.SH_TARGET_CARRIER_VISIT))
        nullifyEntity.put(ArgoEntity.LANE, Arrays.asList(ArgoField.LANE_CARRIER_VISIT))
        nullifyEntity.put(RoadEntity.TRUCK, Arrays.asList(RoadField.TRUCK_DRIVER))
        for (Map.Entry<String, List> entry : nullifyEntity.entrySet()) {
            String key = entry.getKey();
            List<MetafieldId> value = entry.getValue();
            for (MetafieldId nullData : value) {
                clearRefFieldInEntity(key, nullData, inPt);
            }
        }
        final List deleteData = Arrays.asList(MensaEntity.VESSEL_CRANE_STATISTICS, RoadEntity.TRUCK,
                ArgoEntity.CARRIER_VISIT, ServicesEdiEventsEntity.EDI_EVENT, ServicesEntity.APPLICATION_HEALTH_EVENT,
                ServicesEntity.EVENT_FIELD_CHANGE, ServicesEntity.EVENT, InventoryEntity.HAZARDS);
        for (String entity : deleteData) {
            callPurgeEntity("All", entity, inPt);
        }
        logMessage("SpecialPurgeWorkingData: End Purging other entities ******")
    }

    /**
     * Static record to purge the events for each entities
     * @param data
     * @param inPt
     */
    private void purgeEventHelper(String data, PersistenceTemplate inPt) {
        switch (data) {
            case "A":
                purgeEvents(LogicalEntityEnum.UNIT, inPt)
                break;
            case "B":
                purgeEvents(LogicalEntityEnum.VV, inPt)
                break;
            case "C":
                purgeEvents(LogicalEntityEnum.RCARV, inPt);
                purgeEvents(LogicalEntityEnum.RV, inPt);
                break;
            case "D":
                purgeEvents(LogicalEntityEnum.BL, inPt)
                break;
            case "E":
                purgeEvents(LogicalEntityEnum.BKG, inPt)
                purgeEvents(LogicalEntityEnum.LO, inPt)
                purgeEvents(LogicalEntityEnum.DO, inPt)
                purgeEvents(LogicalEntityEnum.ERO, inPt)
                purgeEvents(LogicalEntityEnum.RO, inPt)
                break;
            case "F":
                purgeEvents(LogicalEntityEnum.TV, inPt)
            case "L":
                purgeEvents(LogicalEntityEnum.GAPPT, inPt)
                purgeEvents(LogicalEntityEnum.TAPPT, inPt)
                break
            case "P":
            case "Q":
                purgeEvents(LogicalEntityEnum.SRVO, inPt)
                break;
            case "K":
                purgeEvents(LogicalEntityEnum.GAPPT, inPt)
                break;
            case "R":
                purgeEvents(LogicalEntityEnum.CRGSO, inPt)
                break;
            case "X":
                purgeEvents(LogicalEntityEnum.CVSO, inPt)
                break;
        }
    }

    /**
     * Reference to the carrier visit will be cleared for TV,VV, RV
     * @param data
     * @param pt
     */
    void purgeVisit(String data, PersistenceTemplate pt) {
        DomainQuery dq;
        if (data == "B") {
            dq = QueryUtils.createDomainQuery(ArgoEntity.CARRIER_VISIT)
                    .addDqPredicate(PredicateFactory.isNotNull(ArgoField.CV_OPERATOR))
                    .addDqPredicate(PredicateFactory.eq(ArgoField.CV_CARRIER_MODE, LocTypeEnum.VESSEL))
        } else if (data == "C") {
            dq = QueryUtils.createDomainQuery(ArgoEntity.CARRIER_VISIT)
                    .addDqPredicate(PredicateFactory.isNotNull(ArgoField.CV_OPERATOR))
                    .addDqPredicate(PredicateFactory.eq(ArgoField.CV_CARRIER_MODE, LocTypeEnum.TRAIN))
        } else if (data == "F") {
            dq = QueryUtils.createDomainQuery(ArgoEntity.CARRIER_VISIT)
                    .addDqPredicate(PredicateFactory.isNotNull(ArgoField.CV_OPERATOR))
                    .addDqPredicate(PredicateFactory.eq(ArgoField.CV_CARRIER_MODE, LocTypeEnum.TRUCK))
        } else {
            return;
        }
        clearAllReferenceInEntity(dq, ArgoField.CV_OPERATOR, pt)
    }
    /**
     * Reference method which stores the mapping of the letters to the specific working data.
     *
     * @return
     */
    static Map purgeWorkingDataHelper() {
        final Map<String, List> deleteData = new HashMap<String, List>()
        deleteData.put("A", A)
        deleteData.put("B", B)
        deleteData.put("C", C)
        deleteData.put("D", D)
        deleteData.put("E", E)
        deleteData.put("F", F)
        deleteData.put("G", G)
        deleteData.put("H", H)
        deleteData.put("I", I)
        deleteData.put("J", J)
        deleteData.put("K", K)
        deleteData.put("L", L)
        deleteData.put("M", M)
        deleteData.put("N", N)
        deleteData.put("O", O)
        deleteData.put("P", P)
        deleteData.put("Q", Q)
        deleteData.put("R", R)
        deleteData.put("S", S)
        deleteData.put("T", T)
        deleteData.put("U", U)
        deleteData.put("V", V)
        deleteData.put("W", W)
        deleteData.put("X", X)
        deleteData.put("Y", Y)
        deleteData.put("Z", Z)
        deleteData.put("0", ZERO)
        deleteData.put("1", ONE)
        deleteData.put("2", TWO)
        deleteData.put("3", THREE)
        deleteData.put("4", FOUR)
        deleteData.put("5", FIVE)
        deleteData.put("6", SIX)
        deleteData.put("7", SEVEN)
        deleteData.put("8", EIGHT)
        deleteData.put("9", NINE)
        deleteData.put("#",POUND);
        return deleteData;
    }

    /**
     * Reference method which stores the mapping of letters to the entities to be nullified.
     *
     * @return
     */
    static Map nullWorkingDataHelper() {
        final Map<String, HashMap> nullifyData = new HashMap<String, HashMap>();
        final Map<String, List<MetafieldId>> purgeUnit = new HashMap<String, List<MetafieldId>>();
        final Map<String, List<MetafieldId>> purgeVesselVisit = new HashMap<String, List<MetafieldId>>();
        final Map<String, List<MetafieldId>> purgeBillOfLanding = new HashMap<String, List<MetafieldId>>();
        final Map<String, List<MetafieldId>> purgeItemSvcTypeUnitEvent = new HashMap<String, List<MetafieldId>>();
        final Map<String, List<MetafieldId>> purgeCvServiceType = new HashMap<String, List<MetafieldId>>();
        final Map<String, List<MetafieldId>> purgeGuarantee = new HashMap<String, List<MetafieldId>>();
        final Map<String, List<MetafieldId>> purgeIdo = new HashMap<String, List<MetafieldId>>();
        final Map<String, List<MetafieldId>> purgeCheStatistics = new HashMap<String, List<MetafieldId>>();
        final Map<String, List<MetafieldId>> purgeTruckVisit = new HashMap<String, List<MetafieldId>>();
        final Map<String, List<MetafieldId>> purgeEqOrder = new HashMap<String, List<MetafieldId>>();
        final Map<String, List<MetafieldId>> purgeCargoService = new HashMap<String, List<MetafieldId>>();
        final Map<String, List<MetafieldId>> purgeCargoServiceOrder = new HashMap<String, List<MetafieldId>>();
        purgeCargoService.put(RoadEntity.TRUCK_TRANSACTION, Arrays.asList(RoadField.TRAN_CARGO_SERVICE_ORDER, RoadField.TRAN_CARGO_SERVICE_ORDER_ITEM))
        purgeCargoService.put(RoadApptsEntity.GATE_APPOINTMENT, Arrays.asList(RoadApptsField.GAPPT_CARGO_SERVICE_ORDER, RoadApptsField.GAPPT_CARGO_SERVICE_ORDER_ITEM))
        purgeCargoService.put(RoadArchiveApptsEntity.ARCHIVE_GATE_APPOINTMENT, Arrays.asList(RoadArchiveApptsField.AR_GAPPT_CARGO_SERVICE_ORDER_NBR, RoadArchiveApptsField.AR_GAPPT_CARGO_SERVICE_ORDER_ITEM_ORDER_NBR))
        purgeCargoServiceOrder.put(RoadEntity.TRUCK_TRANSACTION, Arrays.asList(RoadField.TRAN_CARGO_SERVICE_ORDER_ITEM))
        purgeCargoServiceOrder.put(RoadApptsEntity.GATE_APPOINTMENT, Arrays.asList(RoadApptsField.GAPPT_CARGO_SERVICE_ORDER_ITEM))
        purgeCargoServiceOrder.put(RoadArchiveApptsEntity.ARCHIVE_GATE_APPOINTMENT, Arrays.asList(RoadArchiveApptsField.AR_GAPPT_CARGO_SERVICE_ORDER_ITEM_ORDER_NBR))
        purgeUnit.put(MensaEntity.CHE_MOVE_STATISTICS, Arrays.asList(MensaField.CMS_MOVE_EVENT))
        purgeUnit.put(ServicesEntity.FLAG, Arrays.asList(ServicesField.FLAG_ASSOCIATED_EVENT_GKEY))
        purgeUnit.put(OrdersEntity.ITEM_SERVICE_TYPE_UNIT, Arrays.asList(OrdersField.ITMSRVTYPUNIT_EVENT))
        purgeUnit.put(InventoryEntity.UNIT_EQUIPMENT, Arrays.asList(InventoryField.UE_EQUIPMENT_STATE))
        purgeUnit.put(InventoryEntity.UNIT, Arrays.asList(InventoryField.UNIT_RELATED_UNIT,
                InventoryField.UNIT_CARRIAGE_UNIT, InventoryField.UE_DAMAGES))
        purgeVesselVisit.put(ArgoEntity.POINT_OF_WORK, Arrays.asList(ArgoField.POINTOFWORK_VESSEL_VISIT,
                ArgoField.POINTOFWORK_QC_VESSEL_VISIT))
        purgeBillOfLanding.put(InventoryCargoEntity.BL_RELEASE, Arrays.asList(InventoryCargoField.BLREL_REFERENCE));
        purgeBillOfLanding.put(OrdersEntity.SERVICE_ORDER, Arrays.asList(OrdersField.SRVO_BILL_OF_LADING));
        purgeBillOfLanding.put(OrdersEntity.CARGO_SERVICE_ORDER_ITEM, Arrays.asList(OrdersField.CRGSOI_BL_ITEM, OrdersField.CRGSOI_LOT));
        purgeBillOfLanding.put(RoadApptsEntity.GATE_APPOINTMENT, Arrays.asList(RoadApptsField.GAPPT_BL, RoadApptsField.GAPPT_BL_ITEM));
        purgeBillOfLanding.put(RoadEntity.TRUCK_TRANSACTION, Arrays.asList(RoadField.TRAN_BL, RoadField.TRAN_BL_ITEM));
        purgeBillOfLanding.put(RoadEntity.TRUCK_TRANSACTION_CARGO_LOT, Arrays.asList(RoadField.TRANCARGOLOT_CARGO_LOT))
        purgeItemSvcTypeUnitEvent.put(OrdersEntity.ITEM_SERVICE_TYPE_UNIT, Arrays.asList(OrdersField.ITMSRVTYPUNIT_EVENT))
        purgeCvServiceType.put(OrdersEntity.CARRIER_VISIT_SERVICE_TYPE, Arrays.asList(OrdersField.CVSRVTYP_STATUS))
        purgeEqOrder.put(InventoryEntity.UNIT, Arrays.asList(InventoryField.UNIT_ARRIVAL_ORDER_ITEM,
                InventoryField.UNIT_DEPARTURE_ORDER_ITEM))
        purgeEqOrder.put(InventoryEntity.TBD_UNIT, Arrays.asList(InventoryField.TBDU_DEPARTURE_ORDER_ITEM))
        purgeEqOrder.put(RoadEntity.TRUCK_TRANSACTION, Arrays.asList(RoadField.TRAN_EQO, RoadField.TRAN_EQO_ITEM))
        purgeEqOrder.put(RoadApptsEntity.GATE_APPOINTMENT, Arrays.asList(RoadApptsField.GAPPT_ORDER,
                RoadApptsField.GAPPT_ORDER_ITEM))
        purgeEqOrder.put(InventoryEntity.EQ_BASE_ORDER, Arrays.asList(OrdersField.EQO_HAZARDS))
        purgeEqOrder.put(OrdersEntity.EQUIPMENT_ORDER_ITEM, Arrays.asList(OrdersField.EQOI_HAZARDS))
        purgeEqOrder.put(RoadEntity.TRUCKING_COMPANY, Arrays.asList(RoadField.TRKC_DEFAULT_EDO))
        purgeEqOrder.put(OrdersEntity.SERVICE_ORDER, Arrays.asList(OrdersField.SRVO_EQ_BASE_ORDER))
        purgeGuarantee.put(ArgoExtractEntity.GUARANTEE, Arrays.asList(ArgoExtractField.GNTE_RELATED_GUARANTEE))
        purgeCheStatistics.put(MensaEntity.CHE_MOVE_STATISTICS, Arrays.asList(MensaField.CMS_MOVE_EVENT))
        purgeIdo.put(InventoryEntity.UNIT, Arrays.asList(InventoryField.UNIT_IMPORT_DELIVERY_ORDER))
        purgeTruckVisit.put(RoadEntity.TRUCK_VISIT_DETAILS, Arrays.asList(RoadField.TVDTLS_EXIT_LANE,
                RoadField.TVDTLS_EXCHANGE_LANE, RoadField.TVDTLS_ENTRY_LANE, RoadField.TVDTLS_TROUBLE_LANE))
        purgeTruckVisit.put(RoadEntity.GATE_LANE, Arrays.asList(RoadField.GATELN_TRUCK_VISIT))
        nullifyData.put("A", purgeUnit);
        nullifyData.put("B", purgeVesselVisit);
        nullifyData.put("D", purgeBillOfLanding);
        nullifyData.put("F", purgeTruckVisit);
        nullifyData.put("E", purgeEqOrder);
        nullifyData.put("O", purgeCvServiceType);
        nullifyData.put("X", purgeCvServiceType);
        nullifyData.put("P", purgeItemSvcTypeUnitEvent)
        nullifyData.put("Q", purgeItemSvcTypeUnitEvent)
        nullifyData.put("R", purgeCargoService)
        nullifyData.put("S", purgeCargoServiceOrder)
        nullifyData.put("W", purgeIdo)
        nullifyData.put("Y", purgeCheStatistics)
        nullifyData.put("3", purgeItemSvcTypeUnitEvent)
        nullifyData.put("4", purgeGuarantee)
        nullifyData.put("5", purgeGuarantee)
        nullifyData.put("9", purgeVesselVisit)
        return nullifyData;
    }

    /**
     * For service orders, before deleting them the status should be set to NEW
     * @param inPt
     * @param flag
     */
    private void updateOrderStatus(PersistenceTemplate inPt, String flag) {
        inPt.invoke(new CarinaPersistenceCallback() {
            @Override
            protected void doInTransaction() {
                ContextHelper.setThreadDataSource(DataSourceEnum.USER_DBA)
                logMessage("SpecialPurgeWorkingData: update Status for " + flag)
                if (flag.equals(SERVICEORDER)) {
                    DomainQuery servOrderDq = QueryUtils.createDomainQuery(OrdersEntity.SERVICE_ORDER)
                            .addDqPredicate(PredicateFactory.isNotNull(OrdersField.SRVO_STATUS))
                    Serializable[] srvOrderGkey = HibernateApi.getInstance().findPrimaryKeysByDomainQuery(servOrderDq)
                    if (srvOrderGkey.length > 0) {
                        FieldChanges srvFieldChanges = new FieldChanges()
                        srvFieldChanges.setFieldChange(OrdersField.SRVO_STATUS, ServiceOrderStatusEnum.NEW)
                        HibernateApi.getInstance().batchUpdate(OrdersEntity.SERVICE_ORDER, srvOrderGkey, srvFieldChanges)
                    }
                } else if (flag.equals(CVSERVICEORDER)) {
                    DomainQuery cvServOrderDq = QueryUtils.createDomainQuery(OrdersEntity.CARRIER_VISIT_SERVICE_ORDER)
                            .addDqPredicate(PredicateFactory.isNotNull(OrdersField.SRVO_STATUS))
                    Serializable[] cvServOrder = HibernateApi.getInstance().findPrimaryKeysByDomainQuery(cvServOrderDq)
                    if (cvServOrder.length > 0) {
                        FieldChanges cvSrvOrderFieldChange = new FieldChanges()
                        cvSrvOrderFieldChange.setFieldChange(OrdersField.SRVO_STATUS, ServiceOrderStatusEnum.NEW)
                        HibernateApi.getInstance().batchUpdate(OrdersEntity.CARRIER_VISIT_SERVICE_ORDER, cvServOrder, cvSrvOrderFieldChange)
                    }
                } else if (flag.equals(CARGOSERVICEORDER)) {
                    DomainQuery cargoServOrderDq = QueryUtils.createDomainQuery(OrdersEntity.CARGO_SERVICE_ORDER)
                            .addDqPredicate(PredicateFactory.isNotNull(OrdersField.SRVO_STATUS))
                    Serializable[] cargoServOrder = HibernateApi.getInstance().findPrimaryKeysByDomainQuery(cargoServOrderDq)
                    if (cargoServOrder.length > 0) {
                        FieldChanges cargoServOrderFieldChange = new FieldChanges()
                        cargoServOrderFieldChange.setFieldChange(OrdersField.SRVO_STATUS, ServiceOrderStatusEnum.NEW)
                        HibernateApi.getInstance().batchUpdate(OrdersEntity.CARGO_SERVICE_ORDER, cargoServOrder, cargoServOrderFieldChange)

                    }
                } else if (flag.equals(SERVICETYPE)) {
                    DomainQuery dqISTU = QueryUtils.createDomainQuery(OrdersEntity.ITEM_SERVICE_TYPE_UNIT)
                            .addDqPredicate(PredicateFactory.isNotNull(OrdersField.ITMSRVTYPUNIT_STATUS));
                    Serializable[] pkeysISTU = HibernateApi.getInstance().findPrimaryKeysByDomainQuery(dqISTU);
                    if (pkeysISTU.length > 0) {
                        FieldChanges fcsISTU = new FieldChanges();
                        fcsISTU.setFieldChange(OrdersField.ITMSRVTYPUNIT_STATUS, ServiceOrderUnitStatusEnum.NEW);
                        HibernateApi.getInstance().batchUpdate(OrdersEntity.ITEM_SERVICE_TYPE_UNIT, pkeysISTU, fcsISTU);
                    }
                }
            }
        })
    }

    /**
     * Purge entity class to delete the entities based on pkey
     */
    private static class PurgeEachDataByEntity extends CarinaPersistenceCallback {

        PurgeEachDataByEntity(String inEntityName) {
            _entityClass = HiberCache.entityName2EntityClass(inEntityName)
        }

        void setPrimaryKey(Serializable inPrimaryKey) {
            _primaryKey = inPrimaryKey
        }

        @Override
        protected void doInTransaction() {
            ContextHelper.setThreadDataSource(DataSourceEnum.USER_DBA)
            Entity entity = (Entity) HibernateApi.getInstance().load(_entityClass, _primaryKey)
            HibernateApi.getInstance().delete(entity, true)
        }

        private Serializable _primaryKey
        private Class _entityClass
    }

    /**
     * Null entity class to null the fields based on the Pkey
     */
    private static class NullKeyTask extends CarinaPersistenceCallback {

        NullKeyTask(String inEntityName) {
            _entityClass = HiberCache.entityName2EntityClass(inEntityName)
        }

        void setPrimaryKey(Serializable inPrimaryKey) {
            _primaryKey = inPrimaryKey
        }

        void setForiengKey(MetafieldId inReferencingKeyField) {
            _foreignKeyField = inReferencingKeyField
        }

        @Override
        protected void doInTransaction() {
            ContextHelper.setThreadDataSource(DataSourceEnum.USER_DBA)
            Entity e = (Entity) HibernateApi.getInstance().load(_entityClass, _primaryKey)
            e.setFieldValue(_foreignKeyField, null)
            HibernateApi.getInstance().update(e)
        }

        private Serializable _primaryKey
        private Class _entityClass
        private MetafieldId _foreignKeyField
    }

    /**
     * Get total number of records in the table
     * @param inEntityName
     * @return
     */
    private int getCount(String inEntityName) {
        int totalCount;
        DomainQuery dq = QueryUtils.createDomainQuery(inEntityName);
        totalCount = HibernateApi.getInstance().findCountByDomainQuery(dq);
        return totalCount;
    }

    /**
     * Select clause written for Error purging
     * @param inClassName
     * @return
     */
    static Serializable[] batchSelect(String inClassName) {
        Session session = HibernateApi.getInstance() getCurrentSession();
        // Build the HQL

        DomainQuery query = QueryUtils.createDomainQuery(inClassName)
        DomainQuery dq = (DomainQuery) query.createEnhanceableClone()
        //qResult = query.list().toArray(new Serializable[query.list().size()])
        StringBuilder hql = new StringBuilder();
        hql.append("select distinct(e.id) ");
        hql.append(dq.toHqlObjectQueryString("e"))
        Query q = session.createQuery(hql.toString());

        return (Serializable[]) q.list().toArray(new Serializable[q.list().size()]);

    }

    /**
     * Get the library "groovyLog" to write the log message in a separate file
     *
     * @param inMessage the message to be logged
     */
    public void logMessage(String inMessage) {
        groovyLog.write(inMessage);
    }

    private final static String DATAMIGRATION = "DATAMIGRATION"
    private final static String SPECIALPURGE = "SPECIALPURGE"
    private final static String LASTEXTRACTDATE = "LASTEXTRACTDATE"
    private final static List A = Arrays.asList(MensaEntity.EQUIPMENT_BLOCK_VISIT,
            ServicesEntity.VETO, ServicesEntity.FLAG,
            ServicesMovesEntity.MOVE_EVENT, MovesEntity.WORK_INSTRUCTION,
            VesselEntity.LATE_ARRIVAL,
            InventoryEntity.REEFER_RECORD, RoadApptsEntity.GATE_APPOINTMENT,
            InventoryCargoEntity.CARGO_DAMAGE, RoadEntity.DOCUMENT_MESSAGE,
            RoadEntity.DOCUMENT, RoadEntity.TRUCK_TRANSACTION_CARGO_LOT,
            RoadEntity.TRANSACTION_PLACARD, RoadEntity.BUNDLE, RoadEntity.TRUCK_TRANSACTION_BUNDLE, RoadEntity.SCAN_CONTAINER,
            RoadEntity.SCAN_CHASSIS, RoadEntity.ROAD_INSPECTION, RoadEntity.TRUCK_TRANSACTION_STAGE, RoadEntity.TRUCK_TRANSACTION,
            InventoryEntity.UNIT_EQUIP_DAMAGES, InventoryEntity.OBSERVED_PLACARD,
            OrdersEntity.ITEM_SERVICE_TYPE_UNIT, InventoryEntity.UNIT, InventoryEntity.UNIT_COMBO, ArgoEntity.PROPERTY_SOURCE);
    private final static List B = Arrays.asList(
            VesselEntity.VESSEL_VISIT_BERTHING, VesselEntity.VESSEL_VISIT_LINE,
            VesselEntity.VESSEL_STATISTICS_BY_CRANE, VesselEntity.VESSEL_STATISTICS_BY_LINE, MensalyticsEntity.VESSEL_VISIT_STATISTICS,
            VesselEntity.VESSEL_STATISTICS_DELAY, VesselEntity.LATE_ARRIVAL, VesselEntity.VESSEL_VISIT_DETAILS)
    private final static List C = Arrays.asList(RailEntity.RAILCAR_VISIT,RailEntity.TRAIN_VISIT_DETAILS);
    private final
    static List D = Arrays.asList(InventoryCargoEntity.CARGO_LOT, InventoryCargoEntity.BL_ITEM,
            InventoryCargoEntity.BL_GOODS_BL, InventoryCargoEntity.BL_RELEASE, InventoryCargoEntity.BILL_OF_LADING);
    private final static List E = Arrays.asList(OrdersEntity.EQUIPMENT_ORDER_ITEM,
            InventoryEntity.EQ_BASE_ORDER_ITEM, OrdersEntity.BOOKING, OrdersEntity.EQUIPMENT_RECEIVE_ORDER,
            OrdersEntity.EQUIPMENT_DELIVERY_ORDER, OrdersEntity.EQUIPMENT_LOADOUT_ORDER,
            OrdersEntity.RAIL_ORDER, InventoryEntity.EQ_BASE_ORDER)
    private final static List F = Arrays.asList(InventoryCargoEntity.CARGO_DAMAGE, RoadEntity.DOCUMENT_MESSAGE,
            RoadEntity.DOCUMENT, RoadEntity.TRUCK_TRANSACTION_CARGO_LOT,
            RoadEntity.TRANSACTION_PLACARD, RoadEntity.BUNDLE, RoadEntity.ROAD_INSPECTION,
            RoadEntity.TRUCK_TRANSACTION_STAGE, RoadEntity.TRUCK_TRANSACTION_BUNDLE, RoadEntity.TRUCK_TRANSACTION,
            RoadEntity.TRUCK_VISIT_STAGE, RoadEntity.TRUCK_VISIT_STATS, RoadEntity.TRUCK_VISIT_IMAGE,
            RoadEntity.TRUCK_VISIT_DETAILS);
    private final
    static List G = Arrays.asList(RoadEntity.BUNDLE, RoadEntity.ROAD_INSPECTION)
    private final static List H = Arrays.asList(MovesEntity.WORK_INSTRUCTION, InventoryEntity.TBD_UNIT)
    private final
    static List I = Arrays.asList(EdiEntity.EDI_BATCH, EdiEntity.EDI_INTERCHANGE,
            EdiEntity.EDI_QUEUED_INTERCHANGE, ServicesEdiEventsEntity.EDI_EVENT)
    private final static List J = Arrays.asList(RoadEntity.DOCUMENT_MESSAGE)
    private final static List K = Arrays.asList(RoadApptsEntity.GATE_APPOINTMENT)
    private final
    static List L = Arrays.asList(RoadApptsEntity.GATE_APPOINTMENT, RoadApptsEntity.TRUCK_VISIT_APPOINTMENT)
    private final static List M = Arrays.asList(ArgoIntegrationEntity.INTEGRATION_ERROR)
    private final static List N = Arrays.asList(ArgoEntity.ARGO_WEBSERVICE_LOG_ENTRY)
    private final static List O = Arrays.asList(OrdersEntity.CARRIER_VISIT_SERVICE_TYPE)
    private final static List P = Arrays.asList(OrdersEntity.ITEM_SERVICE_TYPE_UNIT,
            OrdersEntity.ITEM_SERVICE_TYPE, OrdersEntity.SERVICE_ORDER_ITEM, OrdersEntity.SERVICE_ORDER)
    private final static List Q = Arrays.asList(OrdersEntity.ITEM_SERVICE_TYPE_UNIT,
            OrdersEntity.ITEM_SERVICE_TYPE, OrdersEntity.SERVICE_ORDER_ITEM)
    private final static List R = Arrays.asList(OrdersEntity.CARGO_SERVICE_ORDER_ADDRESS,
            OrdersEntity.CARGO_SERVICE_ORDER_ITEM, OrdersEntity.CARGO_SERVICE_ORDER)
    private final static List S = Arrays.asList(OrdersEntity.CARGO_SERVICE_ORDER_ITEM)
    private final static List T = Arrays.asList(VesselEntity.LINE_LOAD_LIST)
    private final static List U = Arrays.asList(VesselEntity.LINE_DISCHARGE_LIST)
    private final static List V = Arrays.asList(ArgoExtractEntity.CHARGEABLE_APPOINTMENT_EVENT)
    private final static List W = Arrays.asList(InventoryEntity.IMPORT_DELIVERY_ORDER)
    private final static List X = Arrays.asList(OrdersEntity.CARRIER_VISIT_SERVICE_TYPE,
            OrdersEntity.CARRIER_VISIT_SERVICE_ORDER)
    private final
    static List Y = Arrays.asList(MensaEntity.EQUIPMENT_BLOCK_VISIT,
            MensaEntity.CHE_MOVE_STATISTICS, MensaEntity.MOVE_RECORD)
    private final static List Z = Arrays.asList(MensalyticsEntity.VESSEL_VISIT_STATISTICS)
    private final static List ZERO = Arrays.asList(MensaEntity.CARRIER_STATISTICS)
    private final static List ONE = Arrays.asList(MensalyticsEntity.CRANE_ACTIVITY_STATISTICS)
    private final
    static List TWO = Arrays.asList(ArgoJobEntity.JOB_EXECUTION_LOG)
    private final
    static List THREE = Arrays.asList(OrdersEntity.ITEM_SERVICE_TYPE_UNIT, OrdersEntity.ITEM_SERVICE_TYPE)
    private final static List FOUR = Arrays.asList(ArgoExtractEntity.GUARANTEE)
    private final static List FIVE = Arrays.asList(ArgoExtractEntity.GUARANTEE, ArgoExtractEntity.CHARGEABLE_UNIT_EVENT)
    private final static List SIX = Arrays.asList(ArgoExtractEntity.CHARGEABLE_MARINE_EVENT)
    private final static List SEVEN = Arrays.asList(MovesEntity.WORK_INSTRUCTION, ControlEntity.CRANE_INSTRUCTION, MovesEntity.WORK_QUEUE)
    private final static List EIGHT = Arrays.asList(ArgoEntity.EC_EVENT)
    private final static List NINE = Arrays.asList(ArgoEntity.EC_ALARM)
    private final static List POUND = Arrays.asList(RailEntity.RAILCAR_STATE, RailEntity.RAILCAR);
    private final static String SERVICEORDER = "SERVICEORDER";
    private final static String CARGOSERVICEORDER = "CARGOSERVICEORDER";
    private final static String CVSERVICEORDER = "CVSERVICEORDER";
    private final static String SERVICETYPE = "SERVICETYPE";
    private final static String dataPurge = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static final Logger LOGGER = Logger.getLogger(VITSpecialPurgeWorkingData.class);
    private final static int batchLimit = 1000;
    private static DatabaseHelper _dbHelper;
    Object groovyLog = null;
}