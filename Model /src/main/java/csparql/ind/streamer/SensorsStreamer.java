package csparql.ind.streamer;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.vocab.OWL2Datatype;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import eu.larkc.csparql.cep.api.RdfStream;

public class SensorsStreamer extends RdfStream implements Runnable {

    private long sleepTime;
    private String baseUri;
    private String prop;
    private String excelFilePath;
    private OWLOntology ontology;
    private OWLDataFactory factory;
    private Workbook workbook;
    private Sheet sheet;
    private Iterator<Row> rowIterator;

    public SensorsStreamer(String iri, String baseUri, String prop, long sleepTime,
                           String excelFilePath, OWLOntology ontology2, OWLDataFactory factory) {
        super(iri);

        this.baseUri = baseUri;
        this.prop = prop;
        this.sleepTime = sleepTime;
        this.excelFilePath = excelFilePath;
        this.ontology = ontology2;
        this.factory = factory;

        try {
            FileInputStream fis = new FileInputStream(new File(excelFilePath));
            this.workbook = new XSSFWorkbook(fis);
            this.sheet = workbook.getSheetAt(0);
            this.rowIterator = sheet.iterator();

            // Skip header row if present
            if (rowIterator.hasNext()) {
                rowIterator.next();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

public SensorsStreamer(String iri, String ns, String prop2, long sleepTime2, String excelFilePath2,
                String ontology2, OWLDataFactory factory2) {
    super(iri);
    //TODO Auto-generated constructor stub
}

    @Override
    public void run() {
        int observationIndex = 0;
        int timeIndex = 0;
        int segIndex = 0;
        int img = -1;

        String ns = "http://example.org/anomaly#";
        String pre_SOSAOnt = "http://www.w3.org/ns/sosa/";
        String pre_TIME = "http://www.w3.org/2006/time#";

        OWLClass Sensor = factory.getOWLClass(IRI.create(pre_SOSAOnt + "Sensor"));
        OWLClass Observation = factory.getOWLClass(IRI.create(pre_SOSAOnt + "Observation"));
        OWLClass ObservableProperty = factory.getOWLClass(IRI.create(pre_SOSAOnt + "ObservableProperty"));
        OWLClass Instant = factory.getOWLClass(IRI.create(pre_TIME + "Instant"));

        OWLObjectProperty madeObservation = factory.getOWLObjectProperty(IRI.create(pre_SOSAOnt + "madeObservation"));
        OWLObjectProperty observedProperty = factory.getOWLObjectProperty(IRI.create(pre_SOSAOnt + "observedProperty"));
        OWLDataProperty hasSimpleResult = factory.getOWLDataProperty(IRI.create(pre_SOSAOnt + "hasSimpleResult"));
        OWLObjectProperty hasTime = factory.getOWLObjectProperty(IRI.create(ns, "hasTime"));
        OWLDataProperty inXSDDateTimeStamp = factory.getOWLDataProperty(IRI.create(pre_TIME + "inXSDDateTimeStamp"));

        OWLOntologyManager manager = ontology.getOWLOntologyManager();

        while (rowIterator.hasNext()) {
            try {
                Row row = rowIterator.next();

                // --- Sensor Data ---
                double time = row.getCell(0).getNumericCellValue();
                double current = row.getCell(1).getNumericCellValue();
                double voltage = row.getCell(2).getNumericCellValue();
                double tempAmbiant = row.getCell(3).getNumericCellValue();
                double tempSensor1 = row.getCell(4).getNumericCellValue();
                double tempSensor2 = row.getCell(5).getNumericCellValue();
                double tempSensor3 = row.getCell(6).getNumericCellValue();
                double soc = row.getCell(7).getNumericCellValue();

                Timestamp date = new Timestamp((long) time);

                streamObservation("CurrentSensor", current, date, observationIndex++, timeIndex++, ns,
                        pre_SOSAOnt, pre_TIME, Sensor, Observation, ObservableProperty, Instant, madeObservation,
                        observedProperty, hasSimpleResult, hasTime, inXSDDateTimeStamp, manager);
                streamObservation("Voltage", voltage, date, observationIndex++, timeIndex++, ns,
                        pre_SOSAOnt, pre_TIME, Sensor, Observation, ObservableProperty, Instant, madeObservation,
                        observedProperty, hasSimpleResult, hasTime, inXSDDateTimeStamp, manager);
                streamObservation("TempAmbiant", tempAmbiant, date, observationIndex++, timeIndex++, ns,
                        pre_SOSAOnt, pre_TIME, Sensor, Observation, ObservableProperty, Instant, madeObservation,
                        observedProperty, hasSimpleResult, hasTime, inXSDDateTimeStamp, manager);
                streamObservation("TempSensor1", tempSensor1, date, observationIndex++, timeIndex++, ns,
                        pre_SOSAOnt, pre_TIME, Sensor, Observation, ObservableProperty, Instant, madeObservation,
                        observedProperty, hasSimpleResult, hasTime, inXSDDateTimeStamp, manager);
                streamObservation("TempSensor2", tempSensor2, date, observationIndex++, timeIndex++, ns,
                        pre_SOSAOnt, pre_TIME, Sensor, Observation, ObservableProperty, Instant, madeObservation,
                        observedProperty, hasSimpleResult, hasTime, inXSDDateTimeStamp, manager);
                streamObservation("TempSensor3", tempSensor3, date, observationIndex++, timeIndex++, ns,
                        pre_SOSAOnt, pre_TIME, Sensor, Observation, ObservableProperty, Instant, madeObservation,
                        observedProperty, hasSimpleResult, hasTime, inXSDDateTimeStamp, manager);
                streamObservation("Soc1", soc, date, observationIndex++, timeIndex++, ns,
                        pre_SOSAOnt, pre_TIME, Sensor, Observation, ObservableProperty, Instant, madeObservation,
                        observedProperty, hasSimpleResult, hasTime, inXSDDateTimeStamp, manager);

                // --- Thermal Segmentation Data ---
                double x = row.getCell(8).getNumericCellValue();
                double y = row.getCell(9).getNumericCellValue();
                double area = row.getCell(10).getNumericCellValue();
                double temperature = row.getCell(11).getNumericCellValue();
                String image = new DataFormatter().formatCellValue(row.getCell(12));
                String battery_part = row.getCell(13).getStringCellValue().toLowerCase();

                int trimmedImage = Integer.parseInt(image.replaceAll("\\D+", ""));

                if (trimmedImage != img) {
                    TimeUnit.SECONDS.sleep(sleepTime);
                }

                RdfQuadruple q = new RdfQuadruple(baseUri + "img-" + trimmedImage,
                        baseUri + "hasSegment", baseUri + "seg-" + segIndex, System.currentTimeMillis());
                this.put(q);
                q = new RdfQuadruple(baseUri + "seg-" + segIndex,
                        baseUri + "hasTemperature1",
                        Double.toString(temperature) + "^^http://www.w3.org/2001/XMLSchema#double",
                        System.currentTimeMillis());
                this.put(q);
                q = new RdfQuadruple(baseUri + "seg-" + segIndex,
                        baseUri + "hasXCoordinate",
                        Double.toString(x) + "^^http://www.w3.org/2001/XMLSchema#double",
                        System.currentTimeMillis());
                this.put(q);
                q = new RdfQuadruple(baseUri + "seg-" + segIndex,
                        baseUri + "hasYCoordinate",
                        Double.toString(y) + "^^http://www.w3.org/2001/XMLSchema#double",
                        System.currentTimeMillis());
                this.put(q);
                q = new RdfQuadruple(baseUri + "seg-" + segIndex,
                        baseUri + "hasSize1",
                        Double.toString(area) + "^^http://www.w3.org/2001/XMLSchema#double",
                        System.currentTimeMillis());
                this.put(q);
                q = new RdfQuadruple(baseUri + "seg-" + segIndex,
                        baseUri + "isLocatedIn", baseUri + "battery_" + battery_part,
                        System.currentTimeMillis());
                this.put(q);

                segIndex++;
                img = trimmedImage;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            ontology.getOWLOntologyManager().saveOntology(ontology);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void streamObservation(String propertyName, double value, Timestamp date, int observationIndex, int timeIndex,
                                   String ns, String pre_SOSAOnt, String pre_TIME, OWLClass Sensor, OWLClass Observation,
                                   OWLClass ObservableProperty, OWLClass Instant, OWLObjectProperty madeObservation,
                                   OWLObjectProperty observedProperty, OWLDataProperty hasSimpleResult, OWLObjectProperty hasTime,
                                   OWLDataProperty inXSDDateTimeStamp, OWLOntologyManager manager) {
        try {
            // Stream as RDF quadruples
            RdfQuadruple q = new RdfQuadruple(baseUri + propertyName, baseUri + "madeObservation",
                    baseUri + "S_" + propertyName + "-Obs-" + observationIndex, System.currentTimeMillis());
            this.put(q);
            q = new RdfQuadruple(baseUri + "S_" + propertyName + "-Obs-" + observationIndex,
                    baseUri + "observedProperty", baseUri + propertyName, System.currentTimeMillis());
            this.put(q);
            q = new RdfQuadruple(baseUri + "S_" + propertyName + "-Obs-" + observationIndex,
                    baseUri + "hasSimpleResult",
                    Double.toString(value) + "^^http://www.w3.org/2001/XMLSchema#double",
                    System.currentTimeMillis());
            this.put(q);
            q = new RdfQuadruple(baseUri + "S_" + propertyName + "-Obs-" + observationIndex,
                    baseUri + "hasTime", baseUri + "t-obs-S_" + propertyName + "-" + timeIndex,
                    System.currentTimeMillis());
            this.put(q);
            q = new RdfQuadruple(baseUri + "t-obs-S_" + propertyName + "-" + timeIndex,
                    baseUri + "inXSDDateTime",
                    date.toString() + "^^http://www.w3.org/2001/XMLSchema#dateTimeStamp",
                    System.currentTimeMillis());
            this.put(q);

            // OWL individuals and axioms
            OWLIndividual sensor = factory.getOWLNamedIndividual(IRI.create(ns, propertyName));
            manager.addAxiom(ontology, factory.getOWLClassAssertionAxiom(Sensor, sensor));

            OWLIndividual obs = factory.getOWLNamedIndividual(IRI.create(ns, "S_" + propertyName + "-Obs-" + observationIndex));
            manager.addAxiom(ontology, factory.getOWLClassAssertionAxiom(Observation, obs));

            OWLIndividual property = factory.getOWLNamedIndividual(IRI.create(ns, propertyName));
            manager.addAxiom(ontology, factory.getOWLClassAssertionAxiom(ObservableProperty, property));

            manager.addAxiom(ontology, factory.getOWLObjectPropertyAssertionAxiom(madeObservation, sensor, obs));
            manager.addAxiom(ontology, factory.getOWLObjectPropertyAssertionAxiom(observedProperty, obs, property));

            OWLIndividual time = factory.getOWLNamedIndividual(IRI.create(pre_TIME, "t-obs-S_" + propertyName + "-" + timeIndex));
            manager.addAxiom(ontology, factory.getOWLClassAssertionAxiom(Instant, time));
            manager.addAxiom(ontology, factory.getOWLObjectPropertyAssertionAxiom(hasTime, obs, time));

            OWLLiteral dateLit = factory.getOWLLiteral(date.toString(), OWL2Datatype.XSD_DATE_TIME);
            manager.addAxiom(ontology, factory.getOWLDataPropertyAssertionAxiom(inXSDDateTimeStamp, time, dateLit));

            OWLLiteral valueLit = factory.getOWLLiteral(value);
            manager.addAxiom(ontology, factory.getOWLDataPropertyAssertionAxiom(hasSimpleResult, obs, valueLit));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
