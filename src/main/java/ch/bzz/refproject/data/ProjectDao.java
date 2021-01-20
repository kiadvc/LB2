package ch.bzz.refproject.data;

import ch.bzz.refproject.model.Category;
import ch.bzz.refproject.model.Project;
import ch.bzz.refproject.util.Result;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ProjectDao implements Dao<Project, String>{

    /**
     * gets all projects
     * @return list of projects
     */
    @Override
    public List<Project> getAll() {
        ResultSet resultSet;
        List<Project> projectList = new ArrayList<>();
        String sqlQuery =
                "SELECT p.startDate, c.title, " +
                        " p.title, p.endDate" +
                        " FROM Project AS p JOIN Category AS c USING (categoryUUID)" +
                        " WHERE status = 'A'" +
                        " ORDER BY c.title ASC" +
                        " ORDER BY p.startDate DESC";
        try {
            resultSet = MySqlDB.sqlSelect(sqlQuery);
            while (resultSet.next()) {
                Project project = new Project();
                setValues(resultSet, project);
                projectList.add(project);
            }

        } catch (SQLException sqlEx) {

            sqlEx.printStackTrace();
            throw new RuntimeException();
        } finally {

            MySqlDB.sqlClose();
        }
        return projectList;

    }

    private void setValues(ResultSet resultSet, Project project) throws SQLException {
        project.setProjectUUID(resultSet.getString("projectUUID"));
        project.setTitle(resultSet.getString("title"));
        project.setStartDate(resultSet.getString("startDate"));
        project.setEndDate(resultSet.getString("endDate"));
        project.setStatus(resultSet.getString("status"));
        project.setCategory(new Category());
        project.getCategory().setCategoryUUID(resultSet.getString("categoryUUID"));

    }

    @Override
    public Project getEntity(String projectUUID) {
        Connection connection;
        PreparedStatement prepStmt;
        ResultSet resultSet;
        Project project = new Project();

        String sqlQuery =
                "SELECT p.projectUUID, p.title, p.title, p.startdate, p.enddate, " +
                        "  p.status" +
                        "  FROM Project AS p JOIN Project AS c USING (categoryUUID)" +
                        " WHERE projectUUID='" + projectUUID.toString() + "'";
        try {
            connection = MySqlDB.getConnection();
            prepStmt = connection.prepareStatement(sqlQuery);
            resultSet = prepStmt.executeQuery();
            if (resultSet.next()) {
                setValues(resultSet, project);
            }

        } catch (SQLException sqlEx) {

            sqlEx.printStackTrace();
            throw new RuntimeException();
        } finally {
            MySqlDB.sqlClose();
        }
        return project;

    }

    @Override
    public Result delete(String projectUUID) {
        Connection connection;
        PreparedStatement prepStmt;
        String sqlQuery =
                "DELETE FROM Project" +
                        " WHERE projectUUID='" + projectUUID.toString() + "'";
        try {
            connection = MySqlDB.getConnection();
            prepStmt = connection.prepareStatement(sqlQuery);
            int affectedRows = prepStmt.executeUpdate();
            if (affectedRows == 1) {
                return Result.SUCCESS;
            } else if (affectedRows == 0) {
                return Result.NOACTION;
            } else {
                return Result.ERROR;
            }
        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
            throw new RuntimeException();
        }

    }

    /**
     * saves a Project in the table "Project"
     * @param project the project object
     * @return Result code
     */
    @Override
    public Result save(Project project) {
        Connection connection;
        PreparedStatement prepStmt;
        String sqlQuery =
                "REPLACE project" +
                        " SET projectUUID='" + project.getProjectUUID() + "'," +
                        " title='" + project.getTitle() + "'," +
                        " projectUUID='" + project.getProjectUUID() + "'," +
                        " price=" + project.getPrice() + "," +
                        " isbn='" + project.getIsbn() + "'";
        try {
            connection = MySqlDB.getConnection();
            prepStmt = connection.prepareStatement(sqlQuery);
            int affectedRows = prepStmt.executeUpdate();
            if (affectedRows <= 2) {
                return Result.SUCCESS;
            } else if (affectedRows == 0) {
                return Result.NOACTION;
            } else {
                return Result.ERROR;
            }
        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
            throw new RuntimeException();
        }

    }

    @Override
    public String toString() {
        return "ProjectDao{}";
    }
}
