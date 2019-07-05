import com.navis.argo.ContextHelper
import com.navis.argo.EdiContainer
import com.navis.argo.VermasTransactionDocument
import com.navis.argo.VermasTransactionsDocument
import com.navis.argo.business.atoms.FreightKindEnum
import com.navis.argo.business.atoms.UnitCategoryEnum
import com.navis.argo.business.reference.Equipment
import com.navis.external.edi.entity.AbstractEdiPostInterceptor
import com.navis.framework.business.Roastery
import com.navis.framework.util.internationalization.PropertyKeyFactory
import com.navis.framework.util.message.MessageCollectorUtils
import com.navis.framework.util.message.MessageLevel
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.units.Unit
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.xmlbeans.XmlObject

class PONLVermasMessageEdiPostInterceptor extends AbstractEdiPostInterceptor {
    @Override
    void beforeEdiPost(XmlObject inXmlTransactionDocument, Map inParams) {
        LOGGER.setLevel(Level.INFO)
        LOGGER.info(" PONLVermasMessageEdiPostInterceptor started execution!!!!!!!!!!")

        VermasTransactionsDocument transactionsDocument = (VermasTransactionsDocument) inXmlTransactionDocument
        VermasTransactionDocument.VermasTransaction vermasTransaction = transactionsDocument.getVermasTransactions().getVermasTransactionArray(0)
        EdiContainer ediContainer = vermasTransaction.getEdiContainer()
        String containerNbr = ediContainer.getContainerNbr()
        LOGGER.info("containerNbr::" + containerNbr)
        if (containerNbr == null) {
            return
        }
        String status = ediContainer.getContainerStatus()
        FreightKindEnum ediFreightKind = status != null ? FreightKindEnum.getEnum(status) : null

        UnitFinder unitFinder = (UnitFinder) Roastery.getBean(UnitFinder.BEAN_ID)
        Equipment equipment = Equipment.findEquipment(containerNbr)
        Unit unit = unitFinder.findActiveUnit(ContextHelper.getThreadComplex(), equipment)
        if (unit != null) {
            LOGGER.info("unit::" + unit)
            FreightKindEnum freightKind = unit.getUnitFreightKind()
            String unitFreightKind = freightKind.getKey()
            LOGGER.info("unitFreightKind::" + unitFreightKind)

            if (ediFreightKind != null && !ediFreightKind.equals(freightKind)) {
                LOGGER.info("coming inside the if condition")
                MessageCollectorUtils.appendMessage(MessageLevel.SEVERE, PropertyKeyFactory.valueOf("VERMAS_MESSAGE_INVALID_FREIGHT_KIND"),
                        "Vermas message will be posted for FCL container but the current unit freight kind is MTY");

            }
        }
    }


    public static Logger LOGGER = Logger.getLogger(PONLVermasMessageEdiPostInterceptor.class)
}
