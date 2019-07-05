import com.navis.argo.ContextHelper
import com.navis.external.framework.ui.lov.AbstractExtensionLovFactory
import com.navis.external.framework.ui.lov.ELovKey
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.persistence.hibernate.CarinaPersistenceCallback
import com.navis.framework.persistence.hibernate.PersistenceTemplate
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.UserContext
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.presentation.FrameworkPresentationUtils
import com.navis.framework.presentation.context.PresentationContextUtils
import com.navis.framework.presentation.lovs.Lov
import com.navis.framework.presentation.lovs.Style
import com.navis.framework.presentation.lovs.list.DomainQueryLov
import com.navis.vessel.VesselEntity
import com.navis.vessel.api.VesselVisitField
import org.apache.log4j.Level
import org.apache.log4j.Logger

class RoutingCustomExtensionLOVFactory extends AbstractExtensionLovFactory {

    @Override
    Lov getLov(ELovKey paramELovKey) {
        LOGGER.setLevel(Level.DEBUG)
        LOGGER.info(" RoutingCustomExtensionLOVFactory started execution")
        if(paramELovKey.represents("CUSTOM_INV016")){
            LOGGER.info("coming inside the loop")
            DomainQuery domainQuery=null
            DomainQueryLov domainQueryLov=null
            PersistenceTemplate pt = new PersistenceTemplate(FrameworkPresentationUtils.getUserContext())
            pt.invoke(new CarinaPersistenceCallback() {
                protected void doInTransaction() {
                    String fct = ContextHelper.getThreadFacility() != null ? ContextHelper.getThreadFacility().getFcyId() : null
                    if(fct != null) {
                        domainQuery = QueryUtils.createDomainQuery(VesselEntity.VESSEL_VISIT_DETAILS)
                                .addDqPredicate(PredicateFactory.eq(VesselVisitField.VVD_FACILITY_ID, fct))
                                .addDqField(VesselVisitField.VVD_ID)
                        domainQueryLov = new DomainQueryLov(domainQuery, Style.LABEL1_DASH_LABEL2)
                    }

                }

            })

            return domainQueryLov

        }

        return null
    }
    public static Logger LOGGER = Logger.getLogger(this.class)
}
