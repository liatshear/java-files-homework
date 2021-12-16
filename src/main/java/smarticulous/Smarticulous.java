package smarticulous;

import smarticulous.db.Exercise;
import smarticulous.db.Submission;
import smarticulous.db.User;
import smarticulous.db.Exercise.Question;

import java.sql.*;

import java.util.ArrayList;
import java.util.List;

import javax.naming.spi.DirStateFactory.Result;

/**
 * The Smarticulous class, implementing a grading system.
 * @param <SQLiteDatabase>
 * @param <Cursor>
 */
public class Smarticulous<SQLiteDatabase, Cursor> {
    //Data base version
    //private static final int DATABASE_VERSION = 1;
    //data base Name
    //private static final String DATABASE_NAME = "UserManager.db";




    /**
     * The connection to the underlying DB.
     * <p>
     * null if the db has not yet been opened.
     */
    Connection db;

    /**
     * Open the {@link Smarticulous} SQLite database.
     * <p>
     * This should open the database, creating a new one if necessary, and set the {@link #db} field
     * to the new connection.
     * <p>
     * The open method should make sure the database contains the following tables, creating them if necessary:
     *
     * <table>
     *   <caption><em>Table name: <strong>User</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>UserId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>Username</td><td>Text</td></tr>
     *   <tr><td>Firstname</td><td>Text</td></tr>
     *   <tr><td>Lastname</td><td>Text</td></tr>
     *   <tr><td>Password</td><td>Text</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Exercise</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>ExerciseId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>Name</td><td>Text</td></tr>
     *   <tr><td>DueDate</td><td>Integer</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Question</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>ExerciseId</td><td>Integer</td></tr>
     *   <tr><td>QuestionId</td><td>Integer</td></tr>
     *   <tr><td>Name</td><td>Text</td></tr>
     *   <tr><td>Desc</td><td>Text</td></tr>
     *   <tr><td>Points</td><td>Integer</td></tr>
     * </table>
     * In this table the combination of ExerciseId and QuestionId together comprise the primary key.
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Submission</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>SubmissionId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>UserId</td><td>Integer</td></tr>
     *   <tr><td>ExerciseId</td><td>Integer</td></tr>
     *   <tr><td>SubmissionTime</td><td>Integer</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>QuestionGrade</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>SubmissionId</td><td>Integer</td></tr>
     *   <tr><td>QuestionId</td><td>Integer</td></tr>
     *   <tr><td>Grade</td><td>Real</td></tr>
     * </table>
     * In this table the combination of SubmissionId and QuestionId together comprise the primary key.
     *
     * @param dburl The JDBC url of the database to open (will be of the form "jdbc:sqlite:...")
     * @return the new connection
     * @throws SQLException
     */


    
    public Connection openDB(String dburl) throws SQLException {
        // create strings to create tables user, exericse, question, submission and questiongrade
        String Usertable = "CREATE TABLE IF NOT EXISTS User (UserId INTEGER PRIMARY KEY, Username TEXT UNIQUE, Firstname TEXT, Lastname TEXT, Password TEXT);";
        String ExerciseTable = "CREATE TABLE IF NOT EXISTS Exercise (ExerciseId INTEGER PRIMARY KEY, Name TEXT, DueDate INTEGER);";
        String QuestionTable = "CREATE TABLE IF NOT EXISTS Question (ExerciseId INTEGER, QuestionId INTEGER, Name TEXT, Desc TEXT, Points INTEGER, PRIMARY KEY (ExerciseId, QuestionId));";
        String SubmissionTable = "CREATE TABLE IF NOT EXISTS Submission (SubmissionId INTEGER PRIMARY KEY, UserId INTEGER, ExerciseId INTEGER, SubmissionTime INTEGER);";
        String QuestionGradeTable = "CREATE TABLE IF NOT EXISTS QuestionGrade (SubmissionId INTEGER, QuestionId INTEGER, Grade REAL, PRIMARY KEY (SubmissionId, QuestionId));";
        try{
            //get connection to dburl
            db = DriverManager.getConnection(dburl);
            Statement st = db.createStatement();
            //create table User, Exercise, Question, Submission,QuestionGrade
            st.execute(Usertable);
            st.execute(ExerciseTable);
            st.execute(QuestionTable);
            st.execute(SubmissionTable);
            st.execute(QuestionGradeTable);
            st.close();
        } catch (SQLException e){
            System.out.println("Error connecting to SQLite database");
        }
        // set auto commit to false and return connection
        db.setAutoCommit(false);
        return db;
    }

    /**
     * Close the DB if it is open.
     *
     * @throws SQLException
     */
    public void closeDB() throws SQLException {
        if (db != null) {
            db.close();
            db = null;
        }
    }


    // =========== User Management =============

    /**
     * Add a user to the database / modify an existing user.
     * <p>
     * Add the user to the database if they don't exist. If a user with user.username does exist,
     * update their password and firstname/lastname in the database.
     *
     * @param user
     * @param password
     * @return the userid.
     * @throws SQLException
     */
    public int addOrUpdateUser(User user, String password) throws SQLException {
        Statement st = db.createStatement();
        String checkUser = "SELECT * FROM User WHERE Username='"+user.username+"';";
        String insertUser = "INSERT INTO User (Username, Firstname, Lastname, Password) " + "VALUES (?, ?, ?, ?);";
        String UpdateUser = "UPDATE User SET Firstname=?, Lastname=?, Password=? WHERE Username=?;";
        String getId = "SELECT UserId FROM User WHERE Username='"+user.username+"';";
        PreparedStatement ps = db.prepareStatement(insertUser);
        PreparedStatement pst = db.prepareStatement(UpdateUser);
        ps.setString(1, user.username);
        ps.setString(2, user.firstname);
        ps.setString(3, user.lastname);
        ps.setString(4, password);
        pst.setString(1, user.firstname);
        pst.setString(2, user.lastname);
        pst.setString(3, password);
        pst.setString(4, user.username);
        ResultSet rs = st.executeQuery(checkUser);
        int userId = 0;
        ResultSet generatedKeys;
        if(!rs.next()){
            ps.executeUpdate();
            generatedKeys = ps.getGeneratedKeys(); 
            if(generatedKeys.next()){
                userId = generatedKeys.getInt(1);
            }
        }
        else{
            pst.executeUpdate();
            generatedKeys = st.executeQuery(getId);
            while(generatedKeys.next()){
                userId = generatedKeys.getInt("UserId");
            }
        }
        generatedKeys.close();
        ps.close();
        pst.close();
        db.commit();
        return userId;
    }


    /**
     * Verify a user's login credentials.
     *
     * @param username
     * @param password
     * @return true if the user exists in the database and the password matches; false otherwise.
     * @throws SQLException
     * <p>
     * Note: this is totally insecure. For real-life password checking, it's important to store only
     * a password hash
     * @see <a href="https://crackstation.net/hashing-security.htm">How to Hash Passwords Properly</a>
     */
    public boolean verifyLogin(String username, String password) throws SQLException {
        Statement stmt = db.createStatement();
        String checkUser = "SELECT Password FROM User WHERE Username='"+ username +"';";
        //check if there is a row in the table User with the same username
        ResultSet rs = stmt.executeQuery(checkUser);
        db.commit();
        while(rs.next()){
        if(rs.getString("Password").equals(password)){
            return true;
        }
    }
        return false;
    }

    // =========== Exercise Management =============

    /**
     * Add an exercise to the database.
     *
     * @param exercise
     * @return the new exercise id, or -1 if an exercise with this id already existed in the database.
     * @throws SQLException
     */
    public int addExercise(Exercise exercise) throws SQLException {
        Statement st = db.createStatement();
        String checkUser = "SELECT * FROM Exercise WHERE ExerciseId='"+exercise.id+"';";
        String insertInto = "INSERT INTO Exercise (ExerciseId, Name, DueDate) VALUES ("+exercise.id+",'"+exercise.name+"',"+exercise.dueDate.getTime()+");";
        PreparedStatement ps = db.prepareStatement(insertInto);
        ResultSet rs = st.executeQuery(checkUser);
        if(!rs.next()){
            ps.executeUpdate(); 
            st.close();
            for(Exercise.Question question: exercise.questions){
                st = db.createStatement();
                String insertQuestion = "INSERT INTO Question (ExerciseId, Name, Desc, Points) VALUES('"+exercise.id+"', '"+question.name+"','"+question.desc+"',"+question.points+ ");";
                st.executeUpdate(insertQuestion);
                st.close();
            }
        }
        else{
            db.commit();
            return -1;
        }
        db.commit();
        ResultSet generatedKeys = ps.getGeneratedKeys();
        int exId = 0;
        if(generatedKeys.next()) {
            exId = generatedKeys.getInt(1);
        }
        ps.close();
        generatedKeys.close();
        return exId;
    }
      
    /**
     * Return a list of all the exercises in the database.
     * <p>
     * The list should be sorted by exercise id.
     * @param <MyType>
     *
     * @return list of all exercises.
     * @throws SQLException
     */


    public <MyType> List<Exercise> loadExercises() throws SQLException {

        List<Exercise> returnList = new ArrayList<>();
        Statement st = db.createStatement();
        String GetExercises = "SELECT *, COUNT(QuestionId) FROM Exercise INNER JOIN Question ON " + "Exercise.ExerciseId = Question.ExerciseId GROUP BY Question.ExerciseId ORDER BY Exercise.ExerciseId ASC;";
        String GetQuestions = "SELECT * FROM Question INNER JOIN Exercise ON " + "Exercise.ExerciseId = Question.ExerciseId ORDER BY Exercise.ExerciseId, QuestionId ASC;";
        ResultSet es = st.executeQuery(GetExercises);
        ResultSet qs = st.executeQuery(GetQuestions);
        int numEx = 0;
        while(qs.next()){
            qs.getInt("QuestionId");
            numEx++;
        }
        while(es.next()){
            Statement stmt = db.createStatement();
            Exercise current = new Exercise(es.getInt("ExerciseId"), es.getString("Name"), new Date(es.getInt("DueDate")));
            int cur = 0;
            for(cur = 0; cur < numEx; cur++){
                qs.next();
                current.addQuestion(qs.getString("Name"), qs.getString("Desc"), qs.getInt("Points"));
                db.commit();
            }
            stmt.close();
            db.commit();
            returnList.add(current);
        }
        st.close();
        qs.close();
        es.close();
        db.commit();
        return returnList;
    }

    // ========== Submission Storage ===============

    /**
     * Store a submission in the database.
     * The id field of the submission will be ignored if it is -1.
     * <p>
     * Return -1 if the corresponding user doesn't exist in the database.
     *
     * @param submission
     * @return the submission id.
     * @throws SQLException
     */
    public int storeSubmission(Submission submission) throws SQLException {
        Statement st = db.createStatement();
        String checkUser = "SELECT UserId FROM User WHERE Username='"+submission.user.username+"';"; 
        String GetUserId = "SELECT SubmissionId FROM Submission ORDER BY SubmissionId DESC LIMIT 1;";
        //check if the user exists in the data base
        ResultSet rs = st.executeQuery(checkUser);
        int UserID = 0;
        //if the resullt set is empty then the user doesnt exist and return -1
        if(rs.next()){
            UserID = rs.getInt("UserId");
            rs.close();
            st.close();
            db.commit();
        }
        else{
            return -1;
        }
        // this means that the corresponding user exists in the database
        
            // check if the submission id is not -1 and if no, then insert submission as normal
        int subID = submission.id;
        if (submission.id != -1){
            String InsertSub = "INSERT INTO Submission (SubmissionId, UserId, ExerciseId, SubmissionTime) VALUES("+submission.id+","+ UserID +", "+submission.exercise.id+", "+submission.submissionTime.getTime()+");";
            Statement stmt1 = db.createStatement();
            stmt1.executeUpdate(InsertSub);
            stmt1.close();
            db.commit();
            return subID;
        }
        else{ // this means the submission id == -1 and therefore insert without id field
            Statement stmt2 = db.createStatement();
            String InsertSubNoId = "INSERT INTO Submission (UserId, ExerciseId, SubmissionTime) VALUES("+UserID+","+submission.exercise.id+", "+submission.submissionTime.getTime()+");";
            stmt2.executeUpdate(InsertSubNoId);
            stmt2.close();
            Statement stmt3 = db.createStatement();
            ResultSet getsubId = stmt3.executeQuery(GetUserId);
            if(getsubId.next()){
                subID = getsubId.getInt("SubmissionId");
            }
            stmt3.close();
            getsubId.close();
            db.commit();
            return subID;

        } 
    }


    // ============= Submission Query ===============


    /**
     * Return a prepared SQL statement that, when executed, will
     * return one row for every question of the latest submission for the given exercise by the given user.
     * <p>
     * The rows should be sorted by QuestionId, and each row should contain:
     * - A column named "SubmissionId" with the submission id.
     * - A column named "QuestionId" with the question id,
     * - A column named "Grade" with the grade for that question.
     * - A column named "SubmissionTime" with the time of submission.
     * <p>
     * Parameter 1 of the prepared statement will be set to the User's username, Parameter 2 to the Exercise Id, and
     * Parameter 3 to the number of questions in the given exercise.
     * <p>
     * This will be used by {@link #getLastSubmission(User, Exercise)}
     *
     * @return
     */
    PreparedStatement getLastSubmissionGradesStatement() throws SQLException {
        /**Statement st = db.createStatement();
        String query= SELECT Question
         FROM Exercise USING (ExerciseId)
               Submission Using (SubmissionId)
               User USING (UserId)

        String LatestSub = "SELECT 1 FROM Submission ORDER BY SubmissionTime DESC;";
        String getEx = "SELECT Exercise FROM Submission;";
        String getExid = "SELECT ExerciseId FROM EXERCISE;";
        String getUser = "SELECT"
        String getQs = "SELECT * FROM Question WHERE ExerciseId = "+exercise.id+""
        ORDER BY QuestionId";
        PreparedStatement pt = db.prepareStatement(query);
        pt.executeQuery();*/
        return null;
    }

    /**
     * Return a prepared SQL statement that, when executed, will
     * return one row for every question of the <i>best</i> submission for the given exercise by the given user.
     * The best submission is the one whose point total is maximal.
     * <p>
     * The rows should be sorted by QuestionId, and each row should contain:
     * - A column named "SubmissionId" with the submission id.
     * - A column named "QuestionId" with the question id,
     * - A column named "Grade" with the grade for that question.
     * - A column named "SubmissionTime" with the time of submission.
     * <p>
     * Parameter 1 of the prepared statement will be set to the User's username, Parameter 2 to the Exercise Id, and
     * Parameter 3 to the number of questions in the given exercise.
     * <p>
     * This will be used by {@link #getBestSubmission(User, Exercise)}
     *
     */
    // didnt do bonus
    PreparedStatement getBestSubmissionGradesStatement() throws SQLException {
        // TODO: Implement
        return null;
    }

    /**
     * Return a submission for the given exercise by the given user that satisfies
     * some condition (as defined by an SQL prepared statement).
     * <p>
     * The prepared statement should accept the user name as parameter 1, the exercise id as parameter 2 and a limit on the
     * number of rows returned as parameter 3, and return a row for each question corresponding to the submission, sorted by questionId.
     * <p>
     * Return null if the user has not submitted the exercise (or is not in the database).
     *
     * @param user
     * @param exercise
     * @param stmt
     * @return
     * @throws SQLException
     */
    Submission getSubmission(User user, Exercise exercise, PreparedStatement stmt) throws SQLException {
        stmt.setString(1, user.username);
        stmt.setInt(2, exercise.id);
        stmt.setInt(3, exercise.questions.size());

        ResultSet res = stmt.executeQuery();

        boolean hasNext = res.next();
        if (!hasNext)
            return null;

        int sid = res.getInt("SubmissionId");
        Date submissionTime = new Date(res.getLong("SubmissionTime"));

        float[] grades = new float[exercise.questions.size()];

        for (int i = 0; hasNext; ++i, hasNext = res.next()) {
            grades[i] = res.getFloat("Grade");
        }

        return new Submission(sid, user, exercise, submissionTime, (float[]) grades);
    }

    /**
     * Return the latest submission for the given exercise by the given user.
     * <p>
     * Return null if the user has not submitted the exercise (or is not in the database).
     *
     * @param user
     * @param exercise
     * @return
     * @throws SQLException
     */
    public Submission getLastSubmission(User user, Exercise exercise) throws SQLException {
        return getSubmission(user, exercise, getLastSubmissionGradesStatement());
    }


    /**
     * Return the submission with the highest total grade
     *
     * @param user the user for which we retrieve the best submission
     * @param exercise the exercise for which we retrieve the best submission
     * @return
     * @throws SQLException
     */
    public Submission getBestSubmission(User user, Exercise exercise) throws SQLException {
        return getSubmission(user, exercise, getBestSubmissionGradesStatement());
    }
}
