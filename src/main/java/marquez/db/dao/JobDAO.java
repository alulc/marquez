package marquez.db.dao;

import marquez.api.Job;
import marquez.api.Ownership;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.sqlobject.CreateSqlObject;
import org.jdbi.v3.sqlobject.SqlObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface JobDAO extends SqlObject {
  static final Logger LOG = LoggerFactory.getLogger(JobDAO.class);

  @CreateSqlObject
  OwnershipDAO createOwnershipDAO();

  default void insert(final Job job) {
    try (final Handle handle = getHandle()) {
      handle.useTransaction(
          h -> {
            //
            final int jobId =
                h.createUpdate(
                        "INSERT INTO jobs (name, nominal_time, category, description) "
                            + "VALUES (:name, :nominalTime, :category, :description)")
                    .bindBean(job)
                    .executeAndReturnGeneratedKeys()
                    .mapTo(int.class)
                    .findOnly();
            //
            final int ownerId =
                h.createQuery("SELECT id FROM owners WHERE name=:name")
                    .bind("name", job.getOwner())
                    .mapTo(int.class)
                    .findOnly();
            //
            final int ownershipId =
                createOwnershipDAO().insert(new Ownership(null, null, jobId, ownerId));
            //
            h.createUpdate("UPDATE jobs SET current_ownership = :ownershipId WHERE id = :jobId")
                .bind("ownershipId", ownershipId)
                .bind("jobId", jobId)
                .execute();
            h.commit();
          });
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }
}
