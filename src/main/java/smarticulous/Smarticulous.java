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
        String Usertable = "CREATE TABLE IF NOT EXISTS User (UserId INTEGER PRIMARY KEY, Username TEXT UNIQUE, Firstname TEXT, Lastname TEXT, Password TEXT);";
        String ExerciseTable = "CREATE TABLE IF NOT EXISTS Exercise (ExerciseId INTEGER PRIMARY KEY, Name TEXT, DueDate INTEGER);";
        String QuestionTable = "CREATE TABLE IF NOT EXISTS Question (ExerciseId INTEGER, QuestionId INTEGER, Name TEXT, Desc TEXT, Points INTEGER, PRIMARY KEY (ExerciseId, QuestionId));";
        String SubmissionTable = "CREATE TABLE IF NOT EXISTS Submission (SubmissionId INTEGER PRIMARY KEY, UserId INTEGER, ExerciseId INTEGER, SubmissionTime INTEGER);";
        String QuestionGradeTable = "CREATE TABLE IF NOT EXISTS QuestionGrade (SubmissionId INTEGER, QuestionId INTEGER, Grade REAL, PRIMARY KEY (SubmissionId, QuestionId));";
        try{
            //get connection to dburl
            db = DriverManager.getConnection(dburl);
            Statement st = db.createStatement();
            //create table User
            st.execute(Usertable);
            // create table Exercise
            st.execute(ExerciseTable);
            // create table Question
            st.execute(QuestionTable);
            // create table Submission
            st.execute(SubmissionTable);
            // create table QuestionGrade
            st.execute(QuestionGradeTable);
            st.close();
        } catch (SQLException e){
            System.out.println("Error connecting to SQLite database");
        }// finally {
          //  try{
             //   closeDB();
          //  } catch (SQLException ex){
            //    System.out.println(ex.getMessage());
         //   }
       // }
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
        String insertUser = "INSERT INTO User (Username, Firstname, Lastname, Password) " + "VALUES (?, ?, ?, ?) ON CONFLICT(Username) DO UPDATE SET Firstname=?, Lastname=?, Password=?;";
        PreparedStatement ps = db.prepareStatement(insertUser);
        ps.setString(1, user.username);
        ps.setString(2, user.firstname);
        ps.setString(3, user.lastname);
        ps.setString(4, password);
        ps.setString(5, user.firstname);
        ps.setString(6, user.lastname);
        ps.setString(7, password);
        ps.executeUpdate();
        ResultSet generatedKeys = ps.getGeneratedKeys();
        int userId = 0;
        if(generatedKeys.next()) {
            userId = generatedKeys.getInt(1);
        }
        ps.close();
        generatedKeys.close();
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
        //check if the exercise exists in the database using its id
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
        //check if exercise was added
        //if (rs > 0){
          //  st.close();
            //return exercise.id;
        }
        // if there is a result just return -1 if exercise already exists 
        //else{
          //  st.close();
            //return -1;

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

        List<Exercise> returnList = new ArrayList<Exercise>();
        //ArrayList<Question> questions = new ArrayList<Question>();
        Statement st = db.createStatement();
        String GetExercises = "SELECT * FROM Exercise ORDER BY ExerciseId;";
        String GetQuestions = "SELECT * FROM Question WHERE ExerciseId = ?;";
        PreparedStatement ps = db.prepareStatement(GetQuestions);
        ResultSet rs = st.executeQuery(GetExercises);
        //GetQuestions.setInt()
        //ResultSet qs = st.executeQuery(GetQuestions);
        st.close();
        //ResultSet qs = st.executeQuery(GetQuestions);
        while(rs.next()){
            int exId = rs.getInt("ExerciseId");
            String name = rs.getString("Name");
            Date DueDate = rs.getDate("DueDate");
            Exercise current = new Exercise(exId, name, DueDate);
            Statement stmt = db.createStatement();
            ps.setInt(1, exId);
            ResultSet qs = stmt.executeQuery(GetQuestions);
            while(qs.next()){
                String Qname = rs.getString("name");
                String Qdesc = rs.getString("desc");
                int Qpoints = rs.getInt("points");
                //Exercise.Question q = new Exercise.Question(Qname, Qdesc, Qpoints);
                //questions.add(q);
                current.addQuestion(Qname, Qdesc, Qpoints);
                //Question q = new Question(Qname, Qdesc, Qpoints);
                //current.questions.add(q);
            }
            stmt.close();
            returnList.add(current);
        }
        st.close();
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
        Statement st = this.db.createStatement();
        String checkUser = "SELECT * FROM User WHERE UserId='$submission.id';"; 
        String InsertSubNoId = "INSERT INTO Submission(UserId, ExerciseId) VALUES('$submission.user', '$submission.exercise');";
        String InsertSub = "INSERT INTO Submission(SubmissionId, UserId, ExerciseId) VALUES('$submission.id','$submission.user', '$submission.exercise');";
        //check if the user exists in the data base
        ResultSet rs = st.executeQuery(checkUser); 
        if(!rs.next()){
            st.close();
            return -1;
        }
        else{
            if (submission.id == -1){
                st.executeUpdate(InsertSubNoId);
                st.close();
                return 1;
            } 
            else{
                st.executeUpdate(InsertSub);
                st.close();
                return 1;
            }
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
