import com.navis.argo.ContextHelper
import com.navis.argo.business.atoms.LocTypeEnum
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.portal.EdiExtractDao
import com.navis.argo.business.reference.Equipment
import com.navis.argo.business.reference.SpecialStow
import com.navis.external.edi.entity.AbstractEdiExtractInterceptor
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.inventory.business.api.UnitField
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFacilityVisit
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.jdom.Element


class StowplanEdiExtractInterceptor extends AbstractEdiExtractInterceptor {

    @Override
    Element beforeEdiMap(Map inParams) {
        LOGGER.setLevel(Level.DEBUG)
        LOGGER.info("StowplanEdiExtractInterceptor started execution")
        LOGGER.info("inParams:" + inParams)
        EdiExtractDao inEdiExtractDAO = (EdiExtractDao) inParams.get("DAO");

        Element element = (Element) inParams.get("XML_TRANSACTION");
        LOGGER.info("element::"+element)

        CarrierVisit carrierVisit=CarrierVisit.hydrate(inEdiExtractDAO.getCvGkey())
        LOGGER.info("carrierVisit"+carrierVisit)
        List<Element> ediContainerList=element!=null ? element.getChildren("ediContainer",element.getNamespace()) :null
        LOGGER.info("ediContainer::"+ediContainerList)
        Iterator iterator=ediContainerList.iterator()
        while (iterator.hasNext()){
            Element ediContainerElement=(Element)iterator.next()
            LOGGER.info("ediContainerElement::"+ediContainerElement)
            String containerNbr=ediContainerElement!=null ? ediContainerElement.getAttributeValue("containerNbr",ediContainerElement.getNamespace()):null
            LOGGER.info("containerNbr::"+containerNbr)
            String containerIntendedOutboundId =ediContainerElement!=null ? ediContainerElement.getAttributeValue("containerIntendedOutboundId",ediContainerElement.getNamespace()):null
            LOGGER.info("containerIntendedOutboundId:"+containerIntendedOutboundId)
            CarrierVisit cv = CarrierVisit.findCarrierVisit(ContextHelper.getThreadFacility(), LocTypeEnum.VESSEL, containerIntendedOutboundId)
            LOGGER.info("cv::"+cv)
            Equipment equipment=Equipment.findEquipment(containerNbr)
            LOGGER.info("equipment::"+equipment)

            UnitFacilityVisit facilityVisit=getAllUnits(equipment,cv)
            LOGGER.info("facilityVisit:"+facilityVisit)
            Unit unit=facilityVisit!=null ? facilityVisit.getUfvUnit(): null
            LOGGER.info("unit::"+unit)
              if(unit!=null){
                  SpecialStow specialStow2=unit.getUnitSpecialStow2()
                  LOGGER.info("specialStow2:"+specialStow2)
                  List<Element> specialStowInstructionsList=ediContainerElement.getChildren("specialStowInstructions",ediContainerElement.getNamespace())
                  LOGGER.info("specialStowInstructionsList::"+specialStowInstructionsList)
                  boolean removeAll=ediContainerElement.getChildren("specialStowInstructions",ediContainerElement.getNamespace()).removeAll(specialStowInstructionsList);
                  LOGGER.info("removeAll::"+removeAll)
                  List<Element> afterRemove=ediContainerElement.getChildren("specialStowInstructions",ediContainerElement.getNamespace())
                  LOGGER.info("afterRemove::"+afterRemove)
                  if(specialStow2!=null){
                      String stwId=specialStow2.getStwId()
                      LOGGER.info("stwId"+stwId)
                      String stwDescription=specialStow2.getStwDescription()
                      LOGGER.info("stwDescription"+stwDescription)
                      Element specialStowInstructionsElement = new Element("specialStowInstructions",ediContainerElement.getNamespace());
                      Element id = new Element("id",ediContainerElement.getNamespace());
                      id.setText(stwId);
                      specialStowInstructionsElement.addContent(id);
                      Element description = new Element("description",ediContainerElement.getNamespace());
                      description.setText(stwDescription);
                      specialStowInstructionsElement.addContent(description);
                      ediContainerElement.addContent(specialStowInstructionsElement)
                  }
              }
        }
    }


    private static UnitFacilityVisit getAllUnits(Equipment inPrimaryEq,CarrierVisit inObcarrier) {
        DomainQuery dq = QueryUtils.createDomainQuery("UnitFacilityVisit")
               .addDqPredicate(PredicateFactory.eq(UnitField.UFV_DECLARED_OB_CV, inObcarrier.getCvGkey()))
                .addDqPredicate(PredicateFactory.eq(UnitField.UFV_PRIMARY_EQ, inPrimaryEq.getEqGkey()))
        UnitFacilityVisit unitFacilityVisit=(UnitFacilityVisit)HibernateApi.getInstance().getUniqueEntityByDomainQuery(dq)
        LOGGER.info("unitFacilityVisit"+unitFacilityVisit)
        return unitFacilityVisit
    }

    private static final Logger LOGGER = Logger.getLogger(StowplanEdiExtractInterceptor.class);
}
