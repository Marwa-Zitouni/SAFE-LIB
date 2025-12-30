package csparql.ind;

import java.io.File;
import org.apache.log4j.PropertyConfigurator;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.larkc.csparql.common.utils.CsparqlUtils;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;
import csparql.ind.streamer.SensorsStreamer;

public class App {

    private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {

        try {
            // Configure log4j logger for the CSparql engine
            PropertyConfigurator.configure("log4j_configuration/csparql_readyToGoPack_log4j.properties");

            // Create and initialize CSparql engine
            CsparqlEngineImpl engine = new CsparqlEngineImpl();
            engine.initialize(true);

            String fileOntology = "onto_anomaly.owl";
            String ontologyURI = "http://example.org/anomaly.owl";
            String ns = ontologyURI + "#";
            // Define excelFilePath early, before it is used
            String excelFilePath = "data_test_1.xlsx";

            // Put static model
            engine.putStaticNamedModel(ontologyURI, CsparqlUtils.serializeRDFFile(fileOntology));

            String queryhighTemp = "REGISTER QUERY highTemp AS "
                    + "PREFIX anomaly: <" + ns + "> "
                    + "PREFIX sosa: <http://www.w3.org/ns/sosa/> "
                    + "PREFIX : <" + ns + "> " 
                    + "SELECT ?c ?vsoc1 "
                    + "FROM STREAM <Stream_TempSensor1> [RANGE 60s STEP 5s] "
                    + "FROM STREAM <Stream_TempSensor2> [RANGE 60s STEP 5s] "
                    + "FROM STREAM <Stream_TempSensor3> [RANGE 60s STEP 5s] "
                    + "FROM STREAM <Stream_CurrentSensor> [RANGE 60s STEP 5s] "
                    + "FROM STREAM <Stream_Soc1> [RANGE 60s STEP 5s] "
                    + "FROM STREAM <Stream_ThermalSensor_1> [RANGE 5s STEP 2s] "
                    + "FROM <" + ontologyURI + "> "
                    + "WHERE { "
                    + "  ?c anomaly:hosts anomaly:TempSensor1 . "
                    + "  anomaly:TempSensor1 anomaly:madeObservation ?o1 . "
                    + "  ?o1 anomaly:hasSimpleResult ?v1 . "
                    + "  ?c anomaly:hosts anomaly:TempSensor2 . "
                    + "  anomaly:TempSensor2 anomaly:madeObservation ?o2 . "
                    + "  ?o2 anomaly:hasSimpleResult ?v2 . "
                    + "  ?c anomaly:hosts anomaly:TempSensor3 . "
                    + "  anomaly:TempSensor3 anomaly:madeObservation ?o3 . "
                    + "  ?o3 anomaly:hasSimpleResult ?v3 . "
                    + "  ?c anomaly:hosts anomaly:CurrentSensor . "
                    + "  anomaly:CurrentSensor anomaly:madeObservation ?oc . "
                    + "  ?oc anomaly:hasSimpleResult ?vc . "
                    + "  ?c anomaly:hosts anomaly:Soc1 . "
                    + "  anomaly:Soc1 anomaly:madeObservation ?osoc1 . "
                    + "  ?osoc1 anomaly:hasSimpleResult ?vsoc1 . "   
                    + "  ?seg :hasTemperature1 ?temp . "
                    + "  FILTER ( (ABS(?v1 - ?v2) > 0.3 || ABS(?v1 - ?v3) > 0.3 || ABS(?v2 - ?v3) > 0.3) "
                    + "  && (?vc > 4.2) "
                    + "  && (?vsoc1 > 0.5) && ?temp > 32.0 )"
                    + "}";

            
                    
 
    
            // Load ontology
            OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
            OWLDataFactory factory = manager.getOWLDataFactory();
            final OWLOntology ontology = manager.loadOntologyFromOntologyDocument(new File(fileOntology));

            // Initialize all streamers with correct parameters
            SensorsStreamer Stream_TempSensor1 = new SensorsStreamer("Stream_TempSensor1", ns, "TempSensor1", 2L, excelFilePath, ontology, factory);
            SensorsStreamer Stream_TempSensor2 = new SensorsStreamer("Stream_TempSensor2", ns, "TempSensor2", 2L, excelFilePath, ontology, factory);
            SensorsStreamer Stream_TempSensor3 = new SensorsStreamer("Stream_TempSensor3", ns, "TempSensor3", 2L, excelFilePath, ontology, factory);
            SensorsStreamer Stream_CurrentSensor = new SensorsStreamer("Stream_CurrentSensor", ns, "CurrentSensor", 2L, excelFilePath, ontology, factory);
            SensorsStreamer Stream_Soc1 = new SensorsStreamer("Stream_Soc1", ns, "Soc1", 2L, excelFilePath, ontology, factory);
            SensorsStreamer Stream_ThermalSensor_1 = new SensorsStreamer("Stream_ThermalSensor_1", ns, "ThermalSensor", 2L, excelFilePath, ontology, factory);

            // Register all streams
            engine.registerStream(Stream_TempSensor1);
            engine.registerStream(Stream_TempSensor2);
            engine.registerStream(Stream_TempSensor3);
            engine.registerStream(Stream_CurrentSensor);
            engine.registerStream(Stream_Soc1);
            engine.registerStream(Stream_ThermalSensor_1);

            // Register query
            CsparqlQueryResultProxy c_highTemp = engine.registerQuery(queryhighTemp, false);
            //CsparqlQueryResultProxy c_Thermal = engine.registerQuery(queryThermal, false);

            // Attach result consumer
            c_highTemp.addObserver(new ConsoleFormatter("highTemp", ns, ontology, factory));
            //c_Thermal.addObserver(new ConsoleFormatter("Thermal", ns, ontology, factory));

            // Start all threads
            new Thread(Stream_TempSensor1).start();
            new Thread(Stream_TempSensor2).start();
            new Thread(Stream_TempSensor3).start();
            new Thread(Stream_CurrentSensor).start();
            new Thread(Stream_Soc1).start();
            new Thread(Stream_ThermalSensor_1).start();

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}