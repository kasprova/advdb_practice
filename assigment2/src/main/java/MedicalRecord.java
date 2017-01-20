import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;
import org.apache.commons.lang.ArrayUtils;

@Entity
@Table(name = "medical_records", schema = "default@hbase_pu")
public class MedicalRecord {
    @Id
    private byte[] id;

    @Column(name="type")
    private String type;

    @Column(name="description")
    private String description;

    @Column(name="date_performed")
    private Date datePerformed;

    public byte[] getId() {
        return id;
    }

    public void setId(byte[] patientId, byte[] recordId) {
        this.id = ArrayUtils.addAll(patientId,recordId);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date getDatePerformed() {
        return datePerformed;
    }

    public void setDatePerformed(Date datePerformed) {
        this.datePerformed = datePerformed;
    }
}
