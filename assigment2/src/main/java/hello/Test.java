package hello;

import com.lits.kundera.test.BaseTest;
import java.io.IOException;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import org.junit.Assert;

public class Test extends BaseTest
{
    @Override
    public void customTest() throws IOException
    {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("hbase_pu");
        EntityManager em = emf.createEntityManager();

        Query findQuery = em.createQuery("select p from hello.Physician p");
        List<Physician> allPhysicians = findQuery.getResultList();
        Assert.assertNotEquals(0, allPhysicians.size());

        Query findQuery2 = em.createQuery("select p from hello.Patient p where p.firstName = Peter");
        List<Patient> numbers = findQuery2.getResultList();
        Assert.assertNotEquals(0, numbers.size());

        Query findQuery3 = em.createQuery("select mr from hello.MedicalRecord mr where mr.type = test");
        List<MedicalRecord> allRecords = findQuery3.getResultList();
        Assert.assertEquals(0, allRecords.size());
    }

}
