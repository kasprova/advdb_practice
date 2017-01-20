import com.lits.kundera.test.Util;
import org.fluttercode.datafactory.impl.DataFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.io.IOException;
import java.util.UUID;

public class EntryPoint
{
    public static void main(String[] args)
    {
        dataGeneration();

        Test test = new Test();
        try
        {
            test.runSuite();
        }
        catch (IOException ex) { }
    }


    private static void dataGeneration()
    {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("hbase_pu");
        EntityManager em = emf.createEntityManager();

        DataFactory df = new DataFactory();

        Patient firstPatient = new Patient();
        UUID firstPatientUUID = UUID.randomUUID();
        byte[] firstPatientId = Util.toBytes(firstPatientUUID);
        firstPatient.setId(firstPatientId);
        firstPatient.setFirstName("Peter");
        firstPatient.setLastName("Smith");
        firstPatient.setDateOfBirth(df.getBirthDate());

        em.persist(firstPatient);


        for (int i = 0; i < 10; i++)
        {
            Physician physician = new Physician();
            UUID physicianUUID = UUID.randomUUID();
            byte[] physicainaId = Util.toBytes(physicianUUID);
            String[] clinics = {"Clinic#05","Clinic#82","Clinic#09","Clinic#52","Clinic#13"};
            String[] spec = {"therapist", "dentist", "ophthalmologist"};
            physician.setId(physicainaId);
            physician.setFullName(df.getFirstName() + " " + df.getLastName());
            physician.setClinicName(df.getItem(clinics));
            physician.setSpecialization(df.getItem(spec));
            for (int j = 0; j < 10; j++)
            {
                Patient patient = new Patient();
                UUID patientUUID = UUID.randomUUID();
                byte[] patientId = Util.toBytes(patientUUID);
                patient.setId(patientId);
                patient.setFirstName(df.getFirstName());
                patient.setLastName(df.getLastName());
                patient.setDateOfBirth(df.getBirthDate());
                for (int k = 0; k < 20; k++)
                {
                    MedicalRecord medicalRecord = new MedicalRecord();
                    UUID medicalRecordUUID = UUID.randomUUID();
                    byte[] medicalRecordId = Util.toBytes(medicalRecordUUID);
                    String[] types = {"visit", "exam", "prescription","other"};
                    medicalRecord.setId(patient.getId(), medicalRecordId);
                    medicalRecord.setDatePerformed(df.getBirthDate());
                    medicalRecord.setDescription(df.getRandomText(5, 50));
                    medicalRecord.setType(df.getItem(types));
                    em.persist(medicalRecord);
                }
                em.persist(patient);
            }
            em.persist(physician);
        }
        em.close();
        emf.close();
    }
}
