package marquez.resources;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import marquez.api.Job;
import marquez.db.dao.JobDAO;

@Path("/jobs")
@Consumes(MediaType.APPLICATION_JSON)
public class JobResource {
  private final JobDAO dao;

  public JobResource(final JobDAO dao) {
    this.dao = dao;
  }

  @POST
  public void createJobs(final Job job) {
    dao.insert(job);
  }
}
